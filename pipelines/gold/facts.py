"""Gold fact tables (grain documented in architecture/data-model.md).

Incremental loading: each fact keeps a watermark on Silver's
``_silver_updated_at`` in ``audit.gold_watermarks``. Only changed natural keys
are recomputed and MERGEd. A fact that depends on several Silver tables (for
example ``fact_orders`` depends on orders, payments, refunds and shipments)
treats a key as changed when any of them changed. The watermark advances
only after the MERGE commits.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.delta_utils import merge_with_set
from pipelines.common.logs import get_logger
from pipelines.common.tables import read_table, table, table_exists

log = get_logger("gold-facts")
EPOCH = datetime(1970, 1, 1)


# ============================================================ watermarks ==
def get_watermark(spark: SparkSession, name: str) -> datetime:
    ref = table("audit.gold_watermarks")
    if not table_exists(spark, ref):
        return EPOCH
    row = read_table(spark, ref).filter(F.col("fact_name") == name).select("watermark").first()
    return row[0] if row and row[0] else EPOCH


def set_watermark(spark: SparkSession, name: str, value: datetime | None, rows: int) -> None:
    if value is None:
        return
    df = spark.createDataFrame(
        [(name, value, rows)], "fact_name STRING, watermark TIMESTAMP, rows_last_run BIGINT"
    )
    df = df.withColumn("updated_at", F.current_timestamp())
    merge_with_set(
        spark, df, table("audit.gold_watermarks"), ["fact_name"], {c: f"s.`{c}`" for c in df.columns}
    )


def changed_keys(
    spark: SparkSession, sources: Iterable[tuple[str, str]], since: datetime
) -> tuple[DataFrame, datetime | None]:
    """Distinct keys changed since the watermark across several Silver tables, plus the new high-water mark."""
    frames, highs = [], []
    for fqn, key in sources:
        if not table_exists(spark, table(fqn)):
            continue
        df = read_table(spark, fqn).filter(F.col("_silver_updated_at") > F.lit(since))
        frames.append(df.select(F.col(key).alias("_key")))
        highs.append(df.agg(F.max("_silver_updated_at")).first()[0])
    keys = frames[0]
    for f in frames[1:]:
        keys = keys.unionByName(f)
    high = max((h for h in highs if h is not None), default=None)
    return keys.filter(F.col("_key").isNotNull()).distinct(), high


def date_key(col: str) -> F.Column:
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


def lookup(
    fact: DataFrame, dim: str, spark: SparkSession, nk_fact: str, nk_dim: str, sk_col: str
) -> DataFrame:
    d = read_table(spark, f"gold.{dim}").select(F.col(nk_dim).alias("_nk"), F.col(sk_col).alias("_sk"))
    return (
        fact.join(F.broadcast(d), F.col(nk_fact) == F.col("_nk"), "left")
        .withColumn(sk_col, F.coalesce(F.col("_sk"), F.lit(-1).cast("long")))
        .drop("_nk", "_sk")
    )


def scd2_lookup(
    fact: DataFrame, dim: str, spark: SparkSession, nk_fact: str, nk_dim: str, sk_col: str, ts_col: str
) -> DataFrame:
    """Surrogate key of the version valid at the fact's timestamp (falls back to the current version)."""
    d = (
        read_table(spark, f"gold.{dim}")
        .filter(F.col(nk_dim) != "UNKNOWN")
        .select(
            F.col(nk_dim).alias("_nk"), F.col(sk_col).alias("_sk"), "valid_from", "valid_to", "is_current"
        )
    )
    at_time = fact.join(
        d,
        (F.col(nk_fact) == F.col("_nk"))
        & (F.col(ts_col) >= F.col("valid_from"))
        & (F.col(ts_col) < F.col("valid_to")),
        "left",
    ).drop("_nk", "valid_from", "valid_to", "is_current")
    current = d.filter("is_current").select(F.col("_nk").alias("_cnk"), F.col("_sk").alias("_csk"))
    return (
        at_time.join(F.broadcast(current), F.col(nk_fact) == F.col("_cnk"), "left")
        .withColumn(sk_col, F.coalesce("_sk", "_csk", F.lit(-1).cast("long")))
        .drop("_sk", "_cnk", "_csk")
    )


def _upsert(
    spark: SparkSession, df: DataFrame, fact: str, keys: list[str], partition_by: list[str] | None = None
) -> int:
    df = df.withColumn("_gold_updated_at", F.current_timestamp()).cache()
    n = df.count()
    if n:
        merge_with_set(
            spark,
            df,
            table(f"gold.{fact}"),
            keys,
            {c: f"s.`{c}`" for c in df.columns},
            partition_by=partition_by or [],
        )
    df.unpersist()
    return n


# ============================================================ fact_orders ==
def load_fact_orders(spark: SparkSession) -> int:
    wm = get_watermark(spark, "fact_orders")
    keys, high = changed_keys(
        spark,
        [
            ("silver.orders", "order_id"),
            ("silver.payments", "order_id"),
            ("silver.refunds", "order_id"),
            ("silver.shipments", "order_id"),
        ],
        wm,
    )
    orders = read_table(spark, "silver.orders").join(keys, F.col("order_id") == F.col("_key"), "left_semi")
    pay = (
        read_table(spark, "silver.payments")
        .join(keys, F.col("order_id") == F.col("_key"), "left_semi")
        .groupBy("order_id")
        .agg(
            F.max((F.col("payment_status") == "captured").cast("int")).cast("boolean").alias("is_paid"),
            F.sum(
                F.when(F.col("payment_status") == "captured", F.col("payment_amount_usd")).otherwise(0)
            ).alias("captured_amount_usd"),
            F.count("*").alias("payment_attempts"),
            F.sum((F.col("payment_status") == "failed").cast("int")).alias("failed_payment_attempts"),
            F.min("captured_at").alias("paid_at"),
        )
    )
    # Refunds are booked at the ORIGINAL order's FX rate (finance convention), so refunding an
    # order in full nets its revenue to exactly zero regardless of later currency moves.
    order_fx = read_table(spark, "silver.orders").select(
        "order_id", F.col("fx_rate_to_usd").alias("_order_fx")
    )
    ref = (
        read_table(spark, "silver.refunds")
        .join(keys, F.col("order_id") == F.col("_key"), "left_semi")
        .join(order_fx, "order_id", "left")
        .groupBy("order_id")
        .agg(
            F.round(F.sum(F.col("refund_amount") / F.col("_order_fx")), 2).alias("refund_amount_usd"),
            F.round(F.sum(F.col("refund_tax_amount") / F.col("_order_fx")), 2).alias("refund_tax_amount_usd"),
            F.count("*").alias("refund_count"),
        )
    )
    f = (
        orders.join(pay, "order_id", "left")
        .join(ref, "order_id", "left")
        .fillna(
            {
                "is_paid": False,
                "captured_amount_usd": 0.0,
                "payment_attempts": 0,
                "failed_payment_attempts": 0,
                "refund_amount_usd": 0.0,
                "refund_tax_amount_usd": 0.0,
                "refund_count": 0,
            }
        )
        .withColumn("promotion_key", F.coalesce("promotion_id", F.lit("NONE")))
    )
    f = scd2_lookup(f, "dim_customer", spark, "customer_id", "customer_id", "customer_sk", "order_timestamp")
    f = lookup(f, "dim_country", spark, "country", "country_code", "country_sk")
    f = lookup(f, "dim_currency", spark, "currency", "currency_code", "currency_sk")
    f = lookup(f, "dim_sales_channel", spark, "sales_channel", "channel_code", "channel_sk")
    f = lookup(f, "dim_promotion", spark, "promotion_key", "promotion_id", "promotion_sk")
    merchandise_usd = F.col("subtotal_amount_usd") - F.col("discount_amount_usd")
    out = f.select(
        "order_id",
        date_key("order_timestamp").alias("order_date_key"),
        F.to_date("order_timestamp").alias("order_date"),
        "order_timestamp",
        "customer_sk",
        "customer_id",
        "country_sk",
        F.col("country").alias("country_code"),
        "currency_sk",
        "currency",
        "channel_sk",
        "sales_channel",
        "promotion_sk",
        "promotion_id",
        "order_status",
        "status_reason",
        "item_count",
        "subtotal_amount",
        "discount_amount",
        "shipping_amount",
        "tax_amount",
        "total_amount",
        "subtotal_amount_usd",
        "discount_amount_usd",
        "shipping_amount_usd",
        "tax_amount_usd",
        "total_amount_usd",
        "fx_rate_to_usd",
        "fx_rate_date",
        "is_paid",
        "paid_at",
        "captured_amount_usd",
        "payment_attempts",
        "failed_payment_attempts",
        "refund_amount_usd",
        "refund_tax_amount_usd",
        "refund_count",
        # GMV: merchandise + shipping value of ALL placed orders, before discounts, excluding tax.
        (F.col("subtotal_amount_usd") + F.col("shipping_amount_usd")).alias("gmv_usd"),
        F.when(F.col("is_paid"), merchandise_usd + F.col("shipping_amount_usd"))
        .otherwise(0.0)
        .alias("gross_revenue_usd"),
        F.when(
            F.col("is_paid"),
            merchandise_usd
            + F.col("shipping_amount_usd")
            - (F.col("refund_amount_usd") - F.col("refund_tax_amount_usd")),
        )
        .otherwise(0.0)
        .alias("net_revenue_usd"),
        (F.col("order_status") == "cancelled").alias("is_cancelled"),
        (F.col("order_status") == "returned").alias("is_fully_returned"),
        "created_at",
        "confirmed_at",
        "cancelled_at",
        "shipped_at",
        "delivered_at",
        "returned_at",
        F.round((F.col("shipped_at").cast("long") - F.col("confirmed_at").cast("long")) / 3600, 2).alias(
            "fulfillment_hours"
        ),
        F.round(
            (F.col("delivered_at").cast("long") - F.col("order_timestamp").cast("long")) / 86400, 2
        ).alias("order_to_delivery_days"),
        "is_late_arrival",
        F.col("_source").alias("source"),
    )
    n = _upsert(spark, out, "fact_orders", ["order_id"])
    set_watermark(spark, "fact_orders", high, n)
    return n


# ======================================================= fact_order_items ==
def load_fact_order_items(spark: SparkSession) -> int:
    wm = get_watermark(spark, "fact_order_items")
    keys, high = changed_keys(spark, [("silver.order_items", "order_id"), ("silver.orders", "order_id")], wm)
    items = read_table(spark, "silver.order_items").join(
        keys, F.col("order_id") == F.col("_key"), "left_semi"
    )
    orders = read_table(spark, "silver.orders").select(
        "order_id",
        "order_timestamp",
        "customer_id",
        "fx_rate_to_usd",
        "order_status",
        "sales_channel",
        "country",
    )
    paid = (
        read_table(spark, "silver.payments")
        .filter(F.col("payment_status") == "captured")
        .select("order_id")
        .distinct()
        .withColumn("is_paid", F.lit(True))
    )
    f = items.join(orders, "order_id", "left").join(paid, "order_id", "left").fillna({"is_paid": False})
    f = scd2_lookup(f, "dim_product", spark, "product_id", "product_id", "product_sk", "order_timestamp")
    prod = (
        read_table(spark, "gold.dim_product")
        .filter("is_current")
        .select(F.col("product_id").alias("_pid"), "category_id", "unit_cost")
    )
    cats = read_table(spark, "gold.dim_category").select(F.col("category_id").alias("_cid"), "category_sk")
    f = f.join(F.broadcast(prod), F.col("product_id") == F.col("_pid"), "left").drop("_pid")
    f = (
        f.join(F.broadcast(cats), F.col("category_id") == F.col("_cid"), "left")
        .drop("_cid")
        .withColumn("category_sk", F.coalesce("category_sk", F.lit(-1).cast("long")))
    )
    f = scd2_lookup(f, "dim_customer", spark, "customer_id", "customer_id", "customer_sk", "order_timestamp")
    rate = F.col("fx_rate_to_usd")
    out = f.select(
        "order_item_id",
        "order_id",
        date_key("order_timestamp").alias("order_date_key"),
        F.to_date("order_timestamp").alias("order_date"),
        "product_sk",
        "product_id",
        "category_sk",
        "category_id",
        "customer_sk",
        "customer_id",
        "sales_channel",
        F.col("country").alias("country_code"),
        "order_status",
        "is_paid",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_gross_amount",
        "line_net_amount",
        "currency",
        F.round(F.col("unit_price") / rate, 2).alias("unit_price_usd"),
        F.round(F.col("line_gross_amount") / rate, 2).alias("line_gross_amount_usd"),
        F.round(F.col("discount_amount") / rate, 2).alias("discount_amount_usd"),
        F.round(F.col("line_net_amount") / rate, 2).alias("line_net_amount_usd"),
        F.round(F.col("unit_cost") * F.col("quantity"), 2).alias("line_cost_usd"),
        F.round(F.col("line_net_amount") / rate - F.col("unit_cost") * F.col("quantity"), 2).alias(
            "line_margin_usd"
        ),
    )
    n = _upsert(spark, out, "fact_order_items", ["order_item_id"])
    set_watermark(spark, "fact_order_items", high, n)
    return n


# ========================================================== simple facts ==
def load_fact_payments(spark: SparkSession) -> int:
    wm = get_watermark(spark, "fact_payments")
    keys, high = changed_keys(spark, [("silver.payments", "payment_id")], wm)
    p = read_table(spark, "silver.payments").join(keys, F.col("payment_id") == F.col("_key"), "left_semi")
    orders = read_table(spark, "silver.orders").select(
        "order_id",
        F.col("total_amount_usd").alias("order_total_usd"),
        "country",
        "sales_channel",
        "customer_id",
    )
    f = p.join(orders, "order_id", "left")
    f = lookup(f, "dim_currency", spark, "currency", "currency_code", "currency_sk")
    f = lookup(f, "dim_country", spark, "country", "country_code", "country_sk")
    out = f.select(
        "payment_id",
        "order_id",
        date_key("payment_timestamp").alias("payment_date_key"),
        F.to_date("payment_timestamp").alias("payment_date"),
        "payment_timestamp",
        "currency_sk",
        "country_sk",
        F.col("country").alias("country_code"),
        "sales_channel",
        "customer_id",
        "payment_method",
        "provider",
        "payment_status",
        "attempt_number",
        "failure_reason",
        "payment_amount",
        "currency",
        "payment_amount_usd",
        "order_total_usd",
        (F.col("payment_status") == "captured").alias("is_success"),
        (F.col("payment_status") == "failed").alias("is_failure"),
        "authorized_at",
        "captured_at",
        "failed_at",
    )
    n = _upsert(spark, out, "fact_payments", ["payment_id"])
    set_watermark(spark, "fact_payments", high, n)
    return n


def load_fact_shipments(spark: SparkSession) -> int:
    wm = get_watermark(spark, "fact_shipments")
    keys, high = changed_keys(spark, [("silver.shipments", "shipment_id")], wm)
    s = read_table(spark, "silver.shipments").join(keys, F.col("shipment_id") == F.col("_key"), "left_semi")
    orders = read_table(spark, "silver.orders").select(
        "order_id", "order_timestamp", "confirmed_at", "sales_channel"
    )
    f = s.join(orders, "order_id", "left")
    f = lookup(f, "dim_warehouse", spark, "warehouse_id", "warehouse_id", "warehouse_sk")
    f = lookup(f, "dim_country", spark, "destination_country", "country_code", "country_sk")
    delivered = F.col("actual_delivery").isNotNull()
    out = f.select(
        "shipment_id",
        "order_id",
        date_key("shipped_at").alias("ship_date_key"),
        F.to_date("shipped_at").alias("ship_date"),
        "warehouse_sk",
        "warehouse_id",
        "country_sk",
        F.col("destination_country").alias("destination_country_code"),
        "carrier",
        "shipment_status",
        "sales_channel",
        "order_timestamp",
        "confirmed_at",
        "shipped_at",
        "estimated_delivery",
        "actual_delivery",
        F.round(
            (F.col("shipped_at").cast("long") - F.coalesce("confirmed_at", "order_timestamp").cast("long"))
            / 3600,
            2,
        ).alias("fulfillment_hours"),
        F.round((F.col("actual_delivery").cast("long") - F.col("shipped_at").cast("long")) / 86400, 2).alias(
            "transit_days"
        ),
        F.round(
            (F.col("actual_delivery").cast("long") - F.col("order_timestamp").cast("long")) / 86400, 2
        ).alias("order_to_delivery_days"),
        delivered.alias("is_delivered"),
        F.when(delivered, F.col("actual_delivery") <= F.col("estimated_delivery")).alias("is_on_time"),
        (
            F.col("had_delay_event")
            | (delivered & (F.col("actual_delivery") > F.col("estimated_delivery")))
            | (~delivered & (F.current_timestamp() > F.col("estimated_delivery")))
        ).alias("is_delayed"),
        F.round(
            F.greatest(
                F.lit(0.0),
                (
                    F.coalesce("actual_delivery", F.current_timestamp()).cast("long")
                    - F.col("estimated_delivery").cast("long")
                )
                / 86400,
            ),
            2,
        ).alias("days_late"),
    ).withColumn("is_sla_breach", F.col("fulfillment_hours") > 48)
    n = _upsert(spark, out, "fact_shipments", ["shipment_id"])
    set_watermark(spark, "fact_shipments", high, n)
    return n


def load_fact_refunds(spark: SparkSession) -> int:
    wm = get_watermark(spark, "fact_refunds")
    keys, high = changed_keys(spark, [("silver.refunds", "refund_id")], wm)
    r = read_table(spark, "silver.refunds").join(keys, F.col("refund_id") == F.col("_key"), "left_semi")
    orders = read_table(spark, "silver.orders").select(
        "order_id",
        "customer_id",
        "country",
        "sales_channel",
        "total_amount_usd",
        F.col("fx_rate_to_usd").alias("_order_fx"),
    )
    f = r.join(orders, "order_id", "left").withColumns(
        {
            # Book at the original order rate (see load_fact_orders); keep the refund-date value for audit.
            "refund_amount_usd_at_refund_date": F.col("refund_amount_usd"),
            "refund_amount_usd": F.round(F.col("refund_amount") / F.col("_order_fx"), 2),
            "refund_tax_amount_usd": F.round(F.col("refund_tax_amount") / F.col("_order_fx"), 2),
        }
    )
    f = lookup(f, "dim_currency", spark, "currency", "currency_code", "currency_sk")
    out = f.select(
        "refund_id",
        "order_id",
        "payment_id",
        "return_id",
        date_key("refund_timestamp").alias("refund_date_key"),
        F.to_date("refund_timestamp").alias("refund_date"),
        "refund_timestamp",
        "currency_sk",
        "currency",
        "customer_id",
        F.col("country").alias("country_code"),
        "sales_channel",
        "refund_reason",
        "refund_status",
        "refund_amount",
        "refund_tax_amount",
        "refund_amount_usd",
        "refund_tax_amount_usd",
        "refund_amount_usd_at_refund_date",
        F.round(F.col("refund_amount_usd") - F.col("refund_tax_amount_usd"), 2).alias(
            "refund_net_of_tax_usd"
        ),
        F.col("total_amount_usd").alias("order_total_usd"),
    )
    n = _upsert(spark, out, "fact_refunds", ["refund_id"])
    set_watermark(spark, "fact_refunds", high, n)
    return n


def load_fact_returns(spark: SparkSession) -> int:
    wm = get_watermark(spark, "fact_returns")
    keys, high = changed_keys(spark, [("silver.returns", "return_id")], wm)
    r = read_table(spark, "silver.returns").join(keys, F.col("return_id") == F.col("_key"), "left_semi")
    items = read_table(spark, "silver.order_items").select(
        "order_item_id", "unit_price", "discount_amount", F.col("quantity").alias("ordered_quantity")
    )
    orders = read_table(spark, "silver.orders").select(
        "order_id", "fx_rate_to_usd", "sales_channel", "country"
    )
    f = r.join(items, "order_item_id", "left").join(orders, "order_id", "left")
    f = scd2_lookup(f, "dim_product", spark, "product_id", "product_id", "product_sk", "requested_at")
    f = lookup(f, "dim_warehouse", spark, "warehouse_id", "warehouse_id", "warehouse_sk")
    out = f.select(
        "return_id",
        "order_id",
        "order_item_id",
        date_key("requested_at").alias("return_date_key"),
        F.to_date("requested_at").alias("return_date"),
        "product_sk",
        "product_id",
        "warehouse_sk",
        "warehouse_id",
        "sales_channel",
        F.col("country").alias("country_code"),
        "return_reason",
        "return_status",
        F.col("quantity").alias("returned_quantity"),
        "requested_at",
        "received_at",
        F.round(
            (
                F.col("quantity") * F.col("unit_price")
                - F.col("discount_amount") * F.col("quantity") / F.col("ordered_quantity")
            )
            / F.col("fx_rate_to_usd"),
            2,
        ).alias("returned_value_usd"),
        F.col("return_reason").isin("damaged", "defective").alias("is_quality_issue"),
    )
    n = _upsert(spark, out, "fact_returns", ["return_id"])
    set_watermark(spark, "fact_returns", high, n)
    return n


def load_fact_clickstream(spark: SparkSession) -> int:
    wm = get_watermark(spark, "fact_clickstream")
    src = read_table(spark, "silver.clickstream").filter(F.col("_silver_updated_at") > F.lit(wm))
    high = src.agg(F.max("_silver_updated_at")).first()[0]
    f = scd2_lookup(
        src, "dim_customer", spark, "customer_id", "customer_id", "customer_sk", "event_timestamp"
    )
    f = scd2_lookup(f, "dim_product", spark, "product_id", "product_id", "product_sk", "event_timestamp")
    f = lookup(f, "dim_sales_channel", spark, "sales_channel", "channel_code", "channel_sk")
    f = lookup(f, "dim_country", spark, "country", "country_code", "country_sk")
    out = f.select(
        "event_id",
        date_key("event_timestamp").alias("event_date_key"),
        "event_date",
        "event_timestamp",
        "event_type",
        "session_id",
        "anonymous_id",
        "customer_sk",
        "customer_id",
        "product_sk",
        "product_id",
        "channel_sk",
        "sales_channel",
        "country_sk",
        F.col("country").alias("country_code"),
        "device_type",
        "browser",
        "traffic_source",
        "campaign_id",
        "search_query",
        "cart_quantity",
        (F.col("event_type") == "product_view").alias("is_product_view"),
        (F.col("event_type") == "add_to_cart").alias("is_add_to_cart"),
        (F.col("event_type") == "checkout_started").alias("is_checkout"),
        (F.col("event_type") == "purchase").alias("is_purchase"),
    )
    n = _upsert(spark, out, "fact_clickstream", ["event_id"], partition_by=["event_date"])
    set_watermark(spark, "fact_clickstream", high, n)
    return n


def load_fact_inventory_snapshot(spark: SparkSession) -> int:
    """Daily snapshot (grain: product x warehouse x day); re-running a day replaces that partition only."""
    inv = read_table(spark, "silver.inventory")
    snap_date = inv.agg(F.max(F.to_date("as_of"))).first()[0]
    f = inv.withColumn("snapshot_date", F.lit(snap_date))
    f = lookup(f, "dim_warehouse", spark, "warehouse_id", "warehouse_id", "warehouse_sk")
    cur = (
        read_table(spark, "gold.dim_product")
        .filter("is_current")
        .select(F.col("product_id").alias("_pid"), "product_sk", "unit_cost", "category_id")
    )
    f = (
        f.join(F.broadcast(cur), F.col("product_id") == F.col("_pid"), "left")
        .drop("_pid")
        .withColumn("product_sk", F.coalesce("product_sk", F.lit(-1).cast("long")))
    )
    out = f.select(
        date_key("snapshot_date").alias("snapshot_date_key"),
        "snapshot_date",
        "product_sk",
        "product_id",
        "category_id",
        "warehouse_sk",
        "warehouse_id",
        "available_quantity",
        "reserved_quantity",
        "on_hand_quantity",
        "reorder_threshold",
        "is_stockout",
        "is_low_stock",
        F.round(F.col("on_hand_quantity") * F.col("unit_cost"), 2).alias("inventory_value_usd"),
        "units_shipped_since_snapshot",
        F.current_timestamp().alias("_gold_updated_at"),
    )
    ref = table("gold.fact_inventory_snapshots")
    writer = out.write.format("delta").partitionBy("snapshot_date")
    if table_exists(spark, ref):
        writer = writer.mode("overwrite").option("replaceWhere", f"snapshot_date = '{snap_date}'")
    else:
        writer = writer.mode("overwrite")
    writer.saveAsTable(ref.qualified_name) if ref.uses_catalog else writer.save(ref.path)
    return out.count()


FACT_LOADERS = {
    "fact_orders": load_fact_orders,
    "fact_order_items": load_fact_order_items,
    "fact_payments": load_fact_payments,
    "fact_shipments": load_fact_shipments,
    "fact_refunds": load_fact_refunds,
    "fact_returns": load_fact_returns,
    "fact_clickstream": load_fact_clickstream,
    "fact_inventory_snapshots": load_fact_inventory_snapshot,
}


def reset_facts(spark: SparkSession, names: list[str]) -> None:
    """Full refresh: drop the fact tables and their watermarks so the next load rebuilds them from Silver."""
    from pipelines.common.tables import drop_table

    for name in names:
        drop_table(spark, table(f"gold.{name}"))
    wm = table("audit.gold_watermarks")
    if table_exists(spark, wm):
        quoted = ", ".join(f"'{n}'" for n in names)
        spark.sql(f"DELETE FROM {wm.qualified_name} WHERE fact_name IN ({quoted})")
    log.warning("facts reset for full refresh", extra={"facts": names})


def load_all_facts(
    spark: SparkSession, names: list[str] | None = None, full_refresh: bool = False
) -> dict[str, int]:
    out = {}
    if full_refresh:
        reset_facts(spark, names or list(FACT_LOADERS))
    for name in names or list(FACT_LOADERS):
        out[name] = FACT_LOADERS[name](spark)
        log.info("fact loaded", extra={"fact": name, "rows_merged": out[name]})
    return out
