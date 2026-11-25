"""Silver jobs: Bronze -> cleaned, conformed, deduplicated Silver tables.

High-volume, event-sourced tables are processed incrementally: a Delta stream
over Bronze with ``foreachBatch`` MERGE. Airflow runs it with
``trigger(availableNow=True)``, and the same code can run continuously.
Reference and master data are small, so they are recomputed from Bronze on
each run and MERGEd (SCD1 in Silver; history in ``silver.customers_history``).
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.delta_utils import append_idempotent, merge_with_set
from pipelines.common.logs import get_logger
from pipelines.common.tables import checkpoint, read_stream_table, read_table, table, table_exists
from pipelines.silver import transforms as T

log = get_logger("silver")


def audit_cols() -> dict[str, Any]:
    return {"_silver_updated_at": F.current_timestamp()}


# =================================================================== util ==
class Context:
    """Reference sets and FX rates loaded once per job run."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.currencies = [
            r[0] for r in read_table(spark, "silver.currencies").select("currency_code").collect()
        ]
        self.countries = [
            r[0] for r in read_table(spark, "silver.countries").select("country_code").collect()
        ]
        self.fx = T.fx_daily(read_table(spark, "silver.exchange_rates"), spark).cache()

    def in_list(self, col: str, values: list[str]) -> str:
        quoted = ", ".join(f"'{v}'" for v in values)
        return f"{col} IN ({quoted})"


def write_rejects(df: DataFrame, entity: str, key: str, source: str, batch_id: int) -> int:
    rejects = T.to_rejects(df, entity, key, source).cache()
    n = rejects.count()
    if n:
        append_idempotent(
            rejects, table("quarantine.silver_rejects"), f"silver-rejects-{entity}-{source}", batch_id
        )
    rejects.unpersist()
    return n


def run_incremental(
    spark: SparkSession,
    name: str,
    source_fqn: str,
    handler: Callable[[DataFrame, int], None],
    once: bool = True,
) -> None:
    """Delta stream over a Bronze table; ``availableNow`` drains new data and stops."""
    if not table_exists(spark, table(source_fqn)):
        log.warning("source table missing, skipping", extra={"job": name, "source": source_fqn})
        return
    writer = (
        read_stream_table(spark, source_fqn)
        .writeStream.queryName(f"silver_{name}")
        .foreachBatch(handler)
        .option("checkpointLocation", checkpoint(f"silver/{name}"))
    )
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime="30 seconds")
    query = writer.start()
    if once:
        query.awaitTermination()
        log.info("incremental job drained", extra={"job": name})


def _add_audit(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("_source", F.lit(source)).withColumns(audit_cols())


# ============================================================= reference ==
def refresh_reference(spark: SparkSession) -> dict[str, int]:
    """SCD1 reference tables from full snapshots in Bronze (latest load wins)."""
    specs: dict[str, tuple[str, list[str], list[str]]] = {
        "countries": (
            "bronze.countries_raw",
            ["country_code"],
            [
                "country_code",
                "country_name",
                "region",
                "currency_code",
                "cast(vat_rate as double) vat_rate",
                "timezone",
            ],
        ),
        "currencies": (
            "bronze.currencies_raw",
            ["currency_code"],
            ["currency_code", "currency_name", "cast(minor_units as int) minor_units"],
        ),
        "categories": (
            "bronze.categories_raw",
            ["category_id"],
            ["category_id", "category_name", "department", "parent_category"],
        ),
        "warehouses": (
            "bronze.warehouses_raw",
            ["warehouse_id"],
            [
                "warehouse_id",
                "warehouse_name",
                "country_code",
                "city",
                "cast(capacity_units as int) capacity_units",
            ],
        ),
        "promotions": (
            "bronze.promotions_raw",
            ["promotion_id"],
            [
                "promotion_id",
                "promotion_name",
                "promotion_type",
                "cast(discount_pct as double) discount_pct",
                "cast(start_date as date) start_date",
                "cast(end_date as date) end_date",
                "channel",
                "department",
            ],
        ),
        "suppliers": (
            "bronze.suppliers_raw",
            ["supplier_id"],
            [
                "supplier_id",
                "supplier_name",
                "country_code",
                "cast(lead_time_days as int) lead_time_days",
                "cast(reliability_score as double) reliability_score",
                "cast(is_active as boolean) is_active",
            ],
        ),
    }
    counts = {}
    for name, (src, keys, exprs) in specs.items():
        if not table_exists(spark, table(src)):
            continue
        df = read_table(spark, src)
        latest_batch = df.agg(F.max("ingestion_timestamp")).first()[0]
        snap = T.latest_by(
            df.filter(F.col("ingestion_timestamp") == latest_batch),
            keys,
            [F.col("ingestion_timestamp").desc()],
        ).selectExpr(*exprs)
        snap = _add_audit(snap, src)
        set_map = {c: f"s.`{c}`" for c in snap.columns}
        merge_with_set(spark, snap, table(f"silver.{name}"), keys, set_map)
        counts[name] = snap.count()
    return counts


def refresh_exchange_rates(spark: SparkSession) -> int:
    raw = read_table(spark, "bronze.exchange_rates_raw")
    parsed = raw.select(
        F.to_date(F.get_json_object("raw_payload", "$.date")).alias("rate_date"),
        F.get_json_object("raw_payload", "$.base").alias("base_currency"),
        F.get_json_object("raw_payload", "$.provider").alias("provider"),
        F.from_json(F.get_json_object("raw_payload", "$.rates"), "map<string,double>").alias("rates"),
        "ingestion_timestamp",
    )
    rates = parsed.select(
        "rate_date",
        "base_currency",
        "provider",
        "ingestion_timestamp",
        F.explode("rates").alias("currency_code", "rate_per_usd"),
    )
    rates = T.apply_rules(
        rates, [("positive_rate", "rate_per_usd > 0"), ("has_date", "rate_date IS NOT NULL")]
    )
    good, bad = T.split_valid(rates)
    good = T.latest_by(good, ["rate_date", "currency_code"], [F.col("ingestion_timestamp").desc()]).drop(
        "ingestion_timestamp"
    )
    good = _add_audit(good, "bronze.exchange_rates_raw")
    merge_with_set(
        spark,
        good,
        table("silver.exchange_rates"),
        ["rate_date", "currency_code"],
        {c: f"s.`{c}`" for c in good.columns},
    )
    if bad.limit(1).count():
        write_rejects(
            bad.withColumn("event_id", F.lit(None).cast("string")),
            "exchange_rates",
            "currency_code",
            "rest",
            0,
        )
    return good.count()


def refresh_marketing(spark: SparkSession) -> int:
    raw = read_table(spark, "bronze.marketing_campaigns_raw")
    schema = (
        "array<struct<campaign_id:string,campaign_name:string,traffic_source:string,spend_date:string,"
        "spend_usd:double,impressions:bigint,clicks:bigint>>"
    )
    rows = raw.select(
        F.explode(F.from_json(F.get_json_object("raw_payload", "$.campaigns"), schema)).alias("c"),
        "ingestion_timestamp",
    ).select("c.*", "ingestion_timestamp")
    rows = rows.withColumn("spend_date", F.to_date("spend_date"))
    rows = T.latest_by(rows, ["campaign_id", "spend_date"], [F.col("ingestion_timestamp").desc()]).drop(
        "ingestion_timestamp"
    )
    rows = _add_audit(rows, "bronze.marketing_campaigns_raw")
    merge_with_set(
        spark,
        rows,
        table("silver.marketing_spend"),
        ["campaign_id", "spend_date"],
        {c: f"s.`{c}`" for c in rows.columns},
    )
    return rows.count()


def refresh_supplier_deliveries(spark: SparkSession) -> int:
    raw = read_table(spark, "bronze.supplier_deliveries_raw")
    rows = T.latest_by(
        raw, ["delivery_id"], [F.col("updated_at").desc(), F.col("ingestion_timestamp").desc()]
    )
    rows = rows.select(
        "delivery_id",
        "supplier_id",
        "warehouse_id",
        "product_id",
        F.col("quantity").cast("int"),
        "ordered_at",
        "expected_at",
        "delivered_at",
        "status",
        "updated_at",
    )
    rows = rows.withColumn("is_late", F.col("delivered_at") > F.col("expected_at")).withColumn(
        "lead_time_days",
        F.round((F.col("delivered_at").cast("long") - F.col("ordered_at").cast("long")) / 86400, 2),
    )
    rows = _add_audit(rows, "bronze.supplier_deliveries_raw")
    merge_with_set(
        spark,
        rows,
        table("silver.supplier_deliveries"),
        ["delivery_id"],
        {c: f"s.`{c}`" for c in rows.columns},
    )
    return rows.count()


# ========================================================= master data ==
CUSTOMER_TRACKED = ["country_code", "customer_segment", "loyalty_status"]


def refresh_customers(spark: SparkSession) -> dict[str, int]:
    """Customers from the operational DB extracts + customer events; masked; history for SCD2."""
    salt = os.environ.get("PII_HASH_SALT", "local-dev-salt")
    cols = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country_code",
        "registration_date",
        "customer_segment",
        "loyalty_status",
        "marketing_opt_in",
        "updated_at",
        "change_ts",
        "_src",
    ]
    frames = []
    if table_exists(spark, table("bronze.customers_raw")):
        ops = read_table(spark, "bronze.customers_raw").select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country_code",
            "registration_date",
            "customer_segment",
            "loyalty_status",
            F.col("marketing_opt_in").cast("boolean"),
            "updated_at",
            F.col("updated_at").alias("change_ts"),
            F.lit("ops_db").alias("_src"),
        )
        frames.append(ops)
    if table_exists(spark, table("bronze.customers_events")):
        ev = T.parse_events(read_table(spark, "bronze.customers_events"), "customers").select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            F.col("country").alias("country_code"),
            "registration_date",
            "customer_segment",
            "loyalty_status",
            "marketing_opt_in",
            "updated_at",
            F.col("event_timestamp").alias("change_ts"),
            F.lit("kafka").alias("_src"),
        )
        frames.append(ev)
    allv = frames[0]
    for f in frames[1:]:
        allv = allv.unionByName(f)
    allv = T.apply_rules(
        allv.select(*cols),
        [
            ("customer_id_present", "customer_id IS NOT NULL"),
            ("email_present", "email IS NOT NULL AND email LIKE '%@%'"),
            ("valid_segment", "customer_segment IN ('consumer','small_business','corporate')"),
            ("valid_loyalty", "loyalty_status IN ('none','bronze','silver','gold','platinum')"),
        ],
    )
    good, bad = T.split_valid(allv)
    if bad.limit(1).count():
        write_rejects(
            bad.withColumn("event_id", F.lit(None).cast("string")).drop("email", "last_name"),
            "customers",
            "customer_id",
            "mixed",
            0,
        )
    good = T.mask_customers(good, salt).cache()

    # Current state (SCD1): latest change per customer.
    current = T.latest_by(good, ["customer_id"], [F.col("change_ts").desc(), F.col("_src")]).drop("_src")
    current = current.withColumnRenamed("change_ts", "last_change_ts")
    current = current.withColumns(audit_cols())
    merge_with_set(
        spark, current, table("silver.customers"), ["customer_id"], {c: f"s.`{c}`" for c in current.columns}
    )

    # Attribute history (input to SCD2 dim_customer): one row per change of a tracked attribute.
    from pyspark.sql import Window

    w = Window.partitionBy("customer_id").orderBy("change_ts", "_src")
    sig = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in CUSTOMER_TRACKED])
    hist = (
        good.withColumn("_sig", sig)
        .withColumn("_prev", F.lag("_sig").over(w))
        .filter(F.col("_prev").isNull() | (F.col("_sig") != F.col("_prev")))
        .select("customer_id", *CUSTOMER_TRACKED, F.col("change_ts").alias("valid_from"))
        .dropDuplicates(["customer_id", "valid_from"])
    )
    hist = hist.withColumns(audit_cols())
    (
        hist.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table("silver.customers_history").path)
    ) if not table("silver.customers_history").uses_catalog else (
        hist.write.format("delta")
        .mode("overwrite")
        .saveAsTable(table("silver.customers_history").qualified_name)
    )
    out = {"customers": current.count(), "customers_history": hist.count(), "customers_rejected": bad.count()}
    good.unpersist()
    return out


def refresh_products(spark: SparkSession) -> int:
    """Products: operational DB + PIM catalogue feed, latest by updated_at, enriched with category & supplier."""
    frames = []
    cols = [
        "product_id",
        "product_name",
        "category_id",
        "supplier_id",
        "brand",
        "unit_price",
        "unit_cost",
        "currency",
        "product_status",
        "weight_kg",
        "updated_at",
        "_src",
    ]
    if table_exists(spark, table("bronze.products_raw")):
        frames.append(
            read_table(spark, "bronze.products_raw").select(
                "product_id",
                "product_name",
                "category_id",
                "supplier_id",
                "brand",
                F.col("unit_price").cast("double"),
                F.col("unit_cost").cast("double"),
                "currency",
                "product_status",
                F.col("weight_kg").cast("double"),
                F.col("updated_at").cast("timestamp"),
                F.lit("ops_db").alias("_src"),
            )
        )
    if table_exists(spark, table("bronze.product_catalog_raw")):
        frames.append(
            read_table(spark, "bronze.product_catalog_raw").select(
                "product_id",
                "product_name",
                "category_id",
                "supplier_id",
                "brand",
                F.col("unit_price").cast("double"),
                F.col("unit_cost").cast("double"),
                "currency",
                "product_status",
                F.col("weight_kg").cast("double"),
                F.to_timestamp("updated_at").alias("updated_at"),
                F.lit("pim_feed").alias("_src"),
            )
        )
    allv = frames[0]
    for f in frames[1:]:
        allv = allv.unionByName(f)
    allv = T.apply_rules(
        allv.select(*cols),
        [
            ("product_id_present", "product_id IS NOT NULL"),
            ("positive_price", "unit_price > 0"),
            ("valid_status", "product_status IN ('active','discontinued','out_of_stock','draft')"),
        ],
    )
    good, bad = T.split_valid(allv)
    if bad.limit(1).count():
        write_rejects(
            bad.withColumn("event_id", F.lit(None).cast("string")), "products", "product_id", "mixed", 0
        )
    latest = T.latest_by(good, ["product_id"], [F.col("updated_at").desc(), F.col("_src")])
    cats = read_table(spark, "silver.categories").select("category_id", "category_name", "department")
    sups = read_table(spark, "silver.suppliers").select("supplier_id", "supplier_name")
    enriched = (
        latest.join(cats, "category_id", "left")
        .join(sups, "supplier_id", "left")
        .withColumn("gross_margin_pct", F.round(1 - F.col("unit_cost") / F.col("unit_price"), 4))
        .withColumnRenamed("_src", "_source")
        .withColumns(audit_cols())
    )
    merge_with_set(
        spark, enriched, table("silver.products"), ["product_id"], {c: f"s.`{c}`" for c in enriched.columns}
    )
    return enriched.count()


# ========================================================= event streams ==
def order_rules(ctx: Context) -> list[tuple[str, str]]:
    return [
        ("order_id_present", "order_id IS NOT NULL"),
        ("customer_id_present", "customer_id IS NOT NULL"),
        ("valid_currency", ctx.in_list("currency", ctx.currencies)),
        ("valid_country", ctx.in_list("country", ctx.countries)),
        (
            "non_negative_amounts",
            "subtotal_amount >= 0 AND discount_amount >= 0 AND shipping_amount >= 0 "
            "AND tax_amount >= 0 AND total_amount >= 0",
        ),
        (
            "total_reconciles",
            "abs(total_amount - (subtotal_amount - discount_amount + shipping_amount + tax_amount)) <= 0.05",
        ),
        (
            "valid_status",
            "order_status IN ('created','confirmed','cancelled','shipped','delivered','returned')",
        ),
    ]


ORDER_MERGE = T.lifecycle_merge_set(
    "status_rank",
    "order_status",
    timestamp_cols=[
        "created_at",
        "confirmed_at",
        "cancelled_at",
        "shipped_at",
        "delivered_at",
        "returned_at",
        "order_timestamp",
    ],
    max_cols=["is_late_arrival"],
    immutable_cols=[
        "customer_id",
        "sales_channel",
        "currency",
        "subtotal_amount",
        "discount_amount",
        "shipping_amount",
        "tax_amount",
        "total_amount",
        "promotion_id",
        "country",
        "session_id",
        "item_count",
        "fx_rate_to_usd",
        "fx_rate_date",
        "subtotal_amount_usd",
        "discount_amount_usd",
        "shipping_amount_usd",
        "tax_amount_usd",
        "total_amount_usd",
    ],
    extra={
        "status_reason": "CASE WHEN s.status_rank >= t.status_rank THEN coalesce(s.status_reason, t.status_reason)"
        " ELSE t.status_reason END",
        "event_count": "t.event_count + s.event_count",
        "_source": "CASE WHEN t._source = s._source THEN t._source ELSE 'mixed' END",
    },
)
ORDER_AMOUNTS = ["subtotal_amount", "discount_amount", "shipping_amount", "tax_amount", "total_amount"]


def _merge_orders(ctx: Context, folded: DataFrame, source: str) -> None:
    out = T.with_usd(folded, ctx.fx, "order_timestamp", "currency", ORDER_AMOUNTS)
    out = out.withColumn("_source", F.lit(source)).withColumns(audit_cols())
    merge_with_set(ctx.spark, out, table("silver.orders"), ["order_id"], ORDER_MERGE)


def orders_events_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        ev = T.apply_rules(T.parse_events(batch, "orders"), order_rules(ctx))
        good, bad = T.split_valid(ev)
        good = good.cache()
        rejected = write_rejects(bad, "orders", "order_id", "kafka", batch_id)
        history = good.select(
            "event_id",
            "order_id",
            "order_status",
            "status_reason",
            "event_timestamp",
            "ingestion_timestamp",
            "is_late_arrival",
        ).withColumns(audit_cols())
        merge_with_set(ctx.spark, history, table("silver.order_status_history"), ["event_id"], None)
        _merge_orders(ctx, T.fold_orders(good), "kafka")
        log.info("orders batch", extra={"batch_id": batch_id, "events": good.count(), "rejected": rejected})
        good.unpersist()

    return handle


def history_orders_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        rows = T.latest_by(batch, ["order_id"], [F.col("ingestion_timestamp").desc()])
        rows = rows.select(
            "order_id",
            "customer_id",
            F.col("order_timestamp").cast("timestamp"),
            "order_status",
            "status_reason",
            "sales_channel",
            "currency",
            *[F.col(c).cast("double").alias(c) for c in ORDER_AMOUNTS],
            "promotion_id",
            "country",
            F.col("item_count").cast("int"),
            F.col("last_status_at").cast("timestamp").alias("last_status_at"),
            F.lit(None).cast("string").alias("event_id"),
        )
        rows = T.apply_rules(rows, order_rules(ctx))
        good, bad = T.split_valid(rows)
        write_rejects(bad, "orders", "order_id", "legacy_export", batch_id)

        def at(status: str) -> Any:
            if status == "created":
                return F.col("order_timestamp")
            return F.when(F.col("order_status") == status, F.col("last_status_at"))

        folded = good.select(
            "order_id",
            "customer_id",
            "order_timestamp",
            T.rank_expr("order_status", T.ORDER_STATUS_RANK).alias("status_rank"),
            "status_reason",
            "sales_channel",
            "currency",
            *ORDER_AMOUNTS,
            "promotion_id",
            "country",
            F.lit(None).cast("string").alias("session_id"),
            "item_count",
            *[
                at(s).alias(f"{s}_at")
                for s in ("created", "confirmed", "cancelled", "shipped", "delivered", "returned")
            ],
            F.col("last_status_at").alias("last_event_at"),
            F.lit(False).alias("is_late_arrival"),
            F.lit(1).cast("long").alias("event_count"),
            "order_status",
        )
        _merge_orders(ctx, folded, "legacy_export")

    return handle


ITEM_RULES = [
    ("order_item_id_present", "order_item_id IS NOT NULL"),
    ("order_id_present", "order_id IS NOT NULL"),
    ("product_id_present", "product_id IS NOT NULL"),
    ("positive_quantity", "quantity > 0"),
    ("non_negative_price", "unit_price >= 0"),
    ("discount_within_line", "discount_amount >= 0 AND discount_amount <= quantity * unit_price + 0.01"),
]


def _merge_items(ctx: Context, rows: DataFrame, source: str, batch_id: int) -> None:
    rows = T.apply_rules(rows, [*ITEM_RULES, ("valid_currency", ctx.in_list("currency", ctx.currencies))])
    good, bad = T.split_valid(rows)
    write_rejects(bad, "order_items", "order_item_id", source, batch_id)
    good = (
        T.latest_by(good, ["order_item_id"], [F.col("event_timestamp").asc_nulls_last()])
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "currency",
            "line_number",
            (F.col("quantity") * F.col("unit_price")).alias("line_gross_amount"),
            F.round(F.col("quantity") * F.col("unit_price") - F.col("discount_amount"), 2).alias(
                "line_net_amount"
            ),
            "event_timestamp",
            F.lit(source).alias("_source"),
        )
        .withColumns(audit_cols())
    )
    merge_with_set(ctx.spark, good, table("silver.order_items"), ["order_item_id"], None)


def items_events_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        ev = T.parse_events(batch, "order_items")
        _merge_items(ctx, ev, "kafka", batch_id)

    return handle


def history_items_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        rows = batch.select(
            "order_item_id",
            "order_id",
            "product_id",
            F.col("quantity").cast("int"),
            F.col("unit_price").cast("double"),
            F.col("discount_amount").cast("double"),
            "currency",
            F.lit(None).cast("int").alias("line_number"),
            F.lit(None).cast("timestamp").alias("event_timestamp"),
            F.lit(None).cast("string").alias("event_id"),
        )
        _merge_items(ctx, rows, "legacy_export", batch_id)

    return handle


def payment_rules(ctx: Context) -> list[tuple[str, str]]:
    return [
        ("payment_id_present", "payment_id IS NOT NULL"),
        ("order_id_present", "order_id IS NOT NULL"),
        ("positive_amount", "payment_amount > 0"),
        ("valid_currency", ctx.in_list("currency", ctx.currencies)),
        ("valid_status", "payment_status IN ('authorized','captured','failed')"),
        ("timestamp_present", "payment_timestamp IS NOT NULL"),
    ]


PAYMENT_MERGE = T.lifecycle_merge_set(
    "status_rank",
    "payment_status",
    timestamp_cols=["authorized_at", "captured_at", "failed_at", "payment_timestamp"],
    max_cols=["attempt_number", "is_late_arrival"],
    immutable_cols=[
        "order_id",
        "payment_method",
        "payment_amount",
        "currency",
        "provider",
        "fx_rate_to_usd",
        "fx_rate_date",
        "payment_amount_usd",
    ],
    extra={"failure_reason": "coalesce(s.failure_reason, t.failure_reason)"},
)


def _merge_payments(ctx: Context, folded: DataFrame, source: str) -> None:
    out = T.with_usd(folded, ctx.fx, "payment_timestamp", "currency", ["payment_amount"])
    out = out.withColumn("_source", F.lit(source)).withColumns(audit_cols())
    merge_with_set(ctx.spark, out, table("silver.payments"), ["payment_id"], PAYMENT_MERGE)


def payments_events_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        ev = T.apply_rules(T.parse_events(batch, "payments"), payment_rules(ctx))
        good, bad = T.split_valid(ev)
        write_rejects(bad, "payments", "payment_id", "kafka", batch_id)
        _merge_payments(ctx, T.fold_payments(good), "kafka")

    return handle


def history_payments_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        rows = T.latest_by(batch, ["payment_id"], [F.col("ingestion_timestamp").desc()]).select(
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
            F.col("payment_amount").cast("double"),
            "currency",
            F.col("payment_timestamp").cast("timestamp"),
            F.col("attempt_number").cast("int"),
            "failure_reason",
            F.lit(None).cast("string").alias("event_id"),
        )
        rows = T.apply_rules(rows, payment_rules(ctx))
        good, bad = T.split_valid(rows)
        write_rejects(bad, "payments", "payment_id", "legacy_export", batch_id)
        folded = good.select(
            "payment_id",
            "order_id",
            "payment_method",
            T.rank_expr("payment_status", T.PAYMENT_STATUS_RANK).alias("status_rank"),
            "payment_status",
            "payment_amount",
            "currency",
            "payment_timestamp",
            F.when(F.col("payment_status") != "failed", F.col("payment_timestamp")).alias("authorized_at"),
            F.when(F.col("payment_status") == "captured", F.col("payment_timestamp")).alias("captured_at"),
            F.when(F.col("payment_status") == "failed", F.col("payment_timestamp")).alias("failed_at"),
            "attempt_number",
            "failure_reason",
            F.lit("legacy").alias("provider"),
            F.col("payment_timestamp").alias("last_event_at"),
            F.lit(False).alias("is_late_arrival"),
        )
        _merge_payments(ctx, folded, "legacy_export")

    return handle


SHIPMENT_MERGE = T.lifecycle_merge_set(
    "status_rank",
    "shipment_status",
    timestamp_cols=["created_at", "shipped_at"],
    max_cols=["estimated_delivery", "actual_delivery", "had_delay_event", "is_late_arrival"],
    immutable_cols=["order_id", "warehouse_id", "carrier", "tracking_number", "destination_country"],
)


def shipments_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        ev = T.apply_rules(
            T.parse_events(batch, "shipments"),
            [
                ("shipment_id_present", "shipment_id IS NOT NULL"),
                ("order_id_present", "order_id IS NOT NULL"),
                ("valid_status", "shipment_status IN ('created','in_transit','delayed','delivered')"),
                (
                    "delivery_after_shipping",
                    "actual_delivery IS NULL OR shipped_at IS NULL OR actual_delivery >= shipped_at",
                ),
            ],
        )
        good, bad = T.split_valid(ev)
        write_rejects(bad, "shipments", "shipment_id", "kafka", batch_id)
        folded = T.fold_shipments(good).withColumn("_source", F.lit("kafka")).withColumns(audit_cols())
        merge_with_set(ctx.spark, folded, table("silver.shipments"), ["shipment_id"], SHIPMENT_MERGE)

    return handle


def inventory_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        ev = T.apply_rules(
            T.parse_events(batch, "inventory"),
            [
                ("movement_id_present", "movement_id IS NOT NULL"),
                ("keys_present", "warehouse_id IS NOT NULL AND product_id IS NOT NULL"),
                (
                    "valid_movement",
                    "movement_type IN ('reserve','release','ship','replenish','return_restock','adjust')",
                ),
            ],
        )
        good, bad = T.split_valid(ev)
        write_rejects(bad, "inventory", "movement_id", "kafka", batch_id)
        rows = (
            T.latest_by(good, ["movement_id"], [F.col("event_timestamp")])
            .select(
                "movement_id",
                "event_id",
                "event_type",
                "warehouse_id",
                "product_id",
                "movement_type",
                "quantity_delta_available",
                "quantity_delta_reserved",
                "available_quantity",
                "reserved_quantity",
                "reorder_threshold",
                "reference_id",
                F.col("event_timestamp").alias("movement_ts"),
                F.to_date("event_timestamp").alias("movement_date"),
                "is_late_arrival",
            )
            .withColumns(audit_cols())
        )
        merge_with_set(
            ctx.spark,
            rows,
            table("silver.inventory_movements"),
            ["movement_id"],
            None,
            partition_by=["movement_date"],
        )

    return handle


def refunds_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        ev = T.apply_rules(
            T.parse_events(batch, "refunds"),
            [
                ("refund_id_present", "refund_id IS NOT NULL"),
                ("order_id_present", "order_id IS NOT NULL"),
                ("positive_amount", "refund_amount > 0"),
                ("tax_within_amount", "refund_tax_amount >= 0 AND refund_tax_amount <= refund_amount"),
                ("valid_currency", ctx.in_list("currency", ctx.currencies)),
            ],
        )
        good, bad = T.split_valid(ev)
        write_rejects(bad, "refunds", "refund_id", "kafka", batch_id)
        rows = T.latest_by(good, ["refund_id"], [F.col("event_timestamp")]).select(
            "refund_id",
            "order_id",
            "payment_id",
            "return_id",
            "refund_amount",
            "refund_tax_amount",
            "currency",
            "refund_reason",
            "refund_status",
            "refund_timestamp",
            "is_late_arrival",
        )
        rows = T.with_usd(
            rows, ctx.fx, "refund_timestamp", "currency", ["refund_amount", "refund_tax_amount"]
        )
        rows = rows.withColumn("_source", F.lit("kafka")).withColumns(audit_cols())
        merge_with_set(ctx.spark, rows, table("silver.refunds"), ["refund_id"], None)

    return handle


RETURN_MERGE = T.lifecycle_merge_set(
    "status_rank",
    "return_status",
    timestamp_cols=["requested_at"],
    max_cols=["received_at"],
    immutable_cols=["order_id", "order_item_id", "product_id", "quantity", "return_reason", "warehouse_id"],
)


def returns_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        ev = T.apply_rules(
            T.parse_events(batch, "returns"),
            [
                ("return_id_present", "return_id IS NOT NULL"),
                ("order_item_present", "order_item_id IS NOT NULL"),
                ("positive_quantity", "quantity > 0"),
            ],
        )
        good, bad = T.split_valid(ev)
        write_rejects(bad, "returns", "return_id", "kafka", batch_id)
        folded = T.fold_returns(good).withColumn("_source", F.lit("kafka")).withColumns(audit_cols())
        merge_with_set(ctx.spark, folded, table("silver.returns"), ["return_id"], RETURN_MERGE)

    return handle


CLICK_EVENT_TYPES = (
    "('page_view','search','product_view','add_to_cart','remove_from_cart','checkout_started','purchase')"
)


def clickstream_handler(ctx: Context) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        # Payload event_type / event_timestamp duplicate the envelope; parse_events keeps the envelope's.
        ev = T.parse_events(batch, "clickstream")
        ev = T.apply_rules(
            ev,
            [
                ("session_present", "session_id IS NOT NULL AND anonymous_id IS NOT NULL"),
                ("valid_event_type", f"event_type IN {CLICK_EVENT_TYPES}"),
                ("valid_country", ctx.in_list("country", ctx.countries)),
                (
                    "product_for_product_events",
                    "event_type NOT IN ('product_view','add_to_cart','remove_from_cart') OR product_id IS NOT NULL",
                ),
            ],
        )
        good, bad = T.split_valid(ev)
        write_rejects(bad, "clickstream", "event_id", "kafka", batch_id)
        clicks = (
            good.select(
                "event_id",
                "event_type",
                "event_timestamp",
                F.to_date("event_timestamp").alias("event_date"),
                "customer_id",
                "anonymous_id",
                "session_id",
                "product_id",
                "device_type",
                "browser",
                "traffic_source",
                "country",
                "page_url",
                "search_query",
                "cart_quantity",
                "campaign_id",
                "sales_channel",
                "is_late_arrival",
            )
            .withColumns(audit_cols())
            .cache()
        )
        merge_with_set(
            ctx.spark, clicks, table("silver.clickstream"), ["event_id"], None, partition_by=["event_date"]
        )
        # Recompute every session touched by this batch (late events extend existing sessions).
        touched = clicks.select("session_id").distinct()
        lo_hi = clicks.agg(F.min("event_date"), F.max("event_date")).first()
        all_clicks = read_table(ctx.spark, "silver.clickstream").filter(
            F.col("event_date").between(F.date_sub(F.lit(lo_hi[0]), 1), F.date_add(F.lit(lo_hi[1]), 1))
        )
        sessions = T.reconstruct_sessions(all_clicks.join(touched, "session_id", "left_semi")).withColumns(
            audit_cols()
        )
        merge_with_set(
            ctx.spark,
            sessions,
            table("silver.customer_sessions"),
            ["session_key"],
            {c: f"s.`{c}`" for c in sessions.columns},
            partition_by=["session_date"],
        )
        clicks.unpersist()

    return handle


def refresh_inventory_position(spark: SparkSession) -> int:
    """Current stock = latest opening snapshot + sum of movement deltas after it (order independent)."""
    snap = read_table(spark, "bronze.inventory_snapshot_raw")
    latest_load = snap.agg(F.max("ingestion_timestamp")).first()[0]
    snap = snap.filter(F.col("ingestion_timestamp") == latest_load).select(
        "warehouse_id",
        "product_id",
        F.col("available_quantity").cast("long").alias("opening_available"),
        F.col("reserved_quantity").cast("long").alias("opening_reserved"),
        F.col("reorder_threshold").cast("int"),
        F.col("updated_at").alias("snapshot_ts"),
    )
    if table_exists(spark, table("silver.inventory_movements")):
        mv = read_table(spark, "silver.inventory_movements").drop("reorder_threshold")
        joined = snap.join(mv, ["warehouse_id", "product_id"], "left").withColumn(
            "_after", F.col("movement_ts") > F.col("snapshot_ts")
        )
        agg = joined.groupBy(
            "warehouse_id",
            "product_id",
            "opening_available",
            "opening_reserved",
            "reorder_threshold",
            "snapshot_ts",
        ).agg(
            F.sum(F.when(F.col("_after"), F.col("quantity_delta_available")).otherwise(0)).alias("d_avail"),
            F.sum(F.when(F.col("_after"), F.col("quantity_delta_reserved")).otherwise(0)).alias("d_res"),
            F.max("movement_ts").alias("last_movement_at"),
            F.sum(
                F.when(
                    F.col("_after") & (F.col("movement_type") == "ship"), -F.col("quantity_delta_reserved")
                ).otherwise(0)
            ).alias("units_shipped_since_snapshot"),
        )
    else:
        agg = snap.withColumns(
            {
                "d_avail": F.lit(0),
                "d_res": F.lit(0),
                "last_movement_at": F.lit(None).cast("timestamp"),
                "units_shipped_since_snapshot": F.lit(0),
            }
        )
    pos = (
        agg.select(
            "warehouse_id",
            "product_id",
            (F.col("opening_available") + F.col("d_avail")).cast("long").alias("available_quantity"),
            (F.col("opening_reserved") + F.col("d_res")).cast("long").alias("reserved_quantity"),
            "reorder_threshold",
            "snapshot_ts",
            "last_movement_at",
            F.col("units_shipped_since_snapshot").cast("long"),
        )
        .withColumns(
            {
                "on_hand_quantity": F.col("available_quantity") + F.col("reserved_quantity"),
                "is_stockout": F.col("available_quantity") <= 0,
                "is_low_stock": (F.col("available_quantity") > 0)
                & (F.col("available_quantity") < F.col("reorder_threshold")),
                "as_of": F.current_timestamp(),
            }
        )
        .withColumns(audit_cols())
    )
    ref = table("silver.inventory")
    writer = pos.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    writer.saveAsTable(ref.qualified_name) if ref.uses_catalog else writer.save(ref.path)
    return pos.count()


# ============================================================ orchestration ==
STREAM_JOBS: dict[str, tuple[str, Callable[[Context], Callable[[DataFrame, int], None]]]] = {
    "orders": ("bronze.orders_events", orders_events_handler),
    "orders_history": ("bronze.historical_orders_raw", history_orders_handler),
    "order_items": ("bronze.order_items_events", items_events_handler),
    "order_items_history": ("bronze.historical_order_items_raw", history_items_handler),
    "payments": ("bronze.payments_events", payments_events_handler),
    "payments_history": ("bronze.historical_payments_raw", history_payments_handler),
    "shipments": ("bronze.shipments_events", shipments_handler),
    "inventory_movements": ("bronze.inventory_events", inventory_handler),
    "refunds": ("bronze.refunds_events", refunds_handler),
    "returns": ("bronze.returns_events", returns_handler),
    "clickstream": ("bronze.clickstream_events", clickstream_handler),
}
BATCH_JOBS = ["reference", "exchange_rates", "marketing", "supplier_deliveries", "customers", "products"]
POST_JOBS = ["inventory"]
ALL_JOBS = BATCH_JOBS + list(STREAM_JOBS) + POST_JOBS


BATCH_HANDLERS: dict[str, Callable[[SparkSession], Any]] = {
    "reference": refresh_reference,
    "exchange_rates": refresh_exchange_rates,
    "marketing": refresh_marketing,
    "supplier_deliveries": refresh_supplier_deliveries,
    "customers": refresh_customers,
    "products": refresh_products,
    "inventory": refresh_inventory_position,
}


def run_job(spark: SparkSession, name: str, once: bool = True) -> Any:
    if name in BATCH_HANDLERS:
        return BATCH_HANDLERS[name](spark)
    source, factory = STREAM_JOBS[name]
    ctx = Context(spark)
    run_incremental(spark, name, source, factory(ctx), once)
    return "drained"
