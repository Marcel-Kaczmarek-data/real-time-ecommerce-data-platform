"""Pure Silver transformations (DataFrame in -> DataFrame out).

All I/O lives in ``pipelines/silver/jobs.py``; everything here is unit-tested
with small in-memory DataFrames (tests/unit/test_silver_transformations.py).

Conventions
-----------
* Event-sourced entities are *folded*: each event contributes facts
  (``<status>_at`` timestamps, the highest lifecycle rank seen). Folding uses
  only commutative operations (min / max / least / greatest), so the result
  does not depend on arrival order. Out-of-order, late and duplicated events
  converge to the same row, and replays are idempotent.
* Row-level validation adds ``_reject_reasons`` (array<string>). Rows with
  reasons go to ``quarantine.silver_rejects``. They are never silently dropped.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from schemas.contracts import spark_ddl

LATE_ARRIVAL_SECONDS = 3600

ORDER_STATUS_RANK = {
    "created": 0,
    "confirmed": 1,
    "cancelled": 2,
    "shipped": 3,
    "delivered": 4,
    "returned": 5,
}
PAYMENT_STATUS_RANK = {"authorized": 0, "captured": 1, "failed": 1}
SHIPMENT_STATUS_RANK = {"created": 0, "in_transit": 1, "delayed": 2, "delivered": 3}
RETURN_STATUS_RANK = {"requested": 0, "received": 1}

VALID_ORDER_TRANSITIONS = {
    ("created", "confirmed"),
    ("created", "cancelled"),
    ("confirmed", "cancelled"),
    ("confirmed", "shipped"),
    ("shipped", "delivered"),
    ("delivered", "returned"),
}


# ----------------------------------------------------------------- helpers --
def rank_expr(col: str, ranks: dict[str, int]) -> Column:
    expr = F.lit(None).cast("int")
    for status, rank in ranks.items():
        expr = F.when(F.col(col) == status, F.lit(rank)).otherwise(expr)
    return expr


def status_for_rank(rank_col: str, ranks: dict[str, int], prefer: str | None = None) -> Column:
    """Inverse of rank_expr (for ties, e.g. captured/failed, the caller supplies the status)."""
    expr = F.lit(None).cast("string")
    for status, rank in sorted(ranks.items(), key=lambda kv: kv[1]):
        if prefer and rank == ranks.get(prefer):
            continue
        expr = F.when(F.col(rank_col) == rank, F.lit(status)).otherwise(expr)
    return expr


def apply_rules(df: DataFrame, rules: Sequence[tuple[str, str]]) -> DataFrame:
    """Add ``_reject_reasons``: names of the rules (name, valid-when SQL) a row violates.

    A rule whose expression evaluates to NULL counts as violated: missing data is invalid.
    """
    checks = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in rules]
    return df.withColumn("_reject_reasons", F.array_compact(F.array(*checks)) if checks else F.array())


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    return df.filter(F.size("_reject_reasons") == 0).drop("_reject_reasons"), df.filter(
        F.size("_reject_reasons") > 0
    )


def to_rejects(df: DataFrame, entity: str, key_col: str, source: str) -> DataFrame:
    """Shape invalid rows for ``quarantine.silver_rejects``."""
    payload_cols = [c for c in df.columns if not c.startswith("_")]
    return df.select(
        F.lit(entity).alias("entity"),
        F.col(key_col).cast("string").alias("record_key"),
        (F.col("event_id") if "event_id" in df.columns else F.lit(None).cast("string")).alias("event_id"),
        F.col("_reject_reasons").alias("reasons"),
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in payload_cols])).alias("record"),
        F.lit(source).alias("source"),
        F.current_timestamp().alias("rejected_at"),
        F.current_date().alias("rejected_date"),
    )


def parse_events(bronze: DataFrame, entity: str) -> DataFrame:
    """Bronze envelope rows -> envelope columns + typed payload columns, deduplicated on event_id."""
    parsed = bronze.select(
        "event_id",
        "event_type",
        "event_version",
        "event_timestamp",
        "correlation_id",
        "ingestion_timestamp",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        F.from_json("raw_payload", spark_ddl(entity)).alias("p"),
    ).dropDuplicates(["event_id"])
    envelope = set(parsed.columns)
    payload_cols = [
        F.col(f"p.{f.name}") for f in parsed.schema["p"].dataType.fields if f.name not in envelope
    ]
    out = parsed.select(*[c for c in parsed.columns if c != "p"], *payload_cols)
    return flag_late_arrivals(out)


def flag_late_arrivals(events: DataFrame, threshold_seconds: int = LATE_ARRIVAL_SECONDS) -> DataFrame:
    """Lateness relative to the stream, not to wall-clock processing time.

    An event is *late* when, at its position in the Kafka partition, the
    partition had already carried an event more than ``threshold_seconds``
    newer. Comparing with ingestion time instead would flag every event of a
    replay or catch-up as late.
    """
    w = (
        Window.partitionBy("kafka_topic", "kafka_partition")
        .orderBy("kafka_offset")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    stream_max = F.max(F.col("event_timestamp")).over(w)
    return events.withColumn(
        "is_late_arrival",
        F.coalesce(
            stream_max.cast("long") - F.col("event_timestamp").cast("long") > threshold_seconds, F.lit(False)
        ),
    )


def latest_by(df: DataFrame, keys: Iterable[str], order_cols: Sequence[Column]) -> DataFrame:
    """Keep the newest row per key (deterministic tie-break on the given order)."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# --------------------------------------------------------------------- FX ---
def fx_daily(rates: DataFrame, spark: SparkSession, until: str | None = None) -> DataFrame:
    """Dense (currency, date) -> rate table with forward fill (and back fill before the first quote).

    ``rates``: rate_date DATE, currency_code STRING, rate_per_usd DOUBLE.
    Output: currency_code, rate_date, rate_per_usd, fx_rate_date (the quote date actually used).
    """
    bounds = rates.agg(F.min("rate_date").alias("lo"), F.max("rate_date").alias("hi")).first()
    if bounds is None or bounds["lo"] is None:
        return spark.createDataFrame(
            [], "currency_code STRING, rate_date DATE, rate_per_usd DOUBLE, fx_rate_date DATE"
        )
    hi = F.greatest(F.lit(bounds["hi"]), F.current_date() if until is None else F.lit(until).cast("date"))
    calendar = spark.range(1).select(F.explode(F.sequence(F.lit(bounds["lo"]), hi)).alias("rate_date"))
    grid = calendar.crossJoin(rates.select("currency_code").distinct())
    joined = grid.join(
        rates.select("currency_code", "rate_date", "rate_per_usd", F.col("rate_date").alias("q")),
        ["currency_code", "rate_date"],
        "left",
    )
    fwd = Window.partitionBy("currency_code").orderBy("rate_date").rowsBetween(Window.unboundedPreceding, 0)
    bwd = Window.partitionBy("currency_code").orderBy("rate_date").rowsBetween(0, Window.unboundedFollowing)
    return joined.select(
        "currency_code",
        "rate_date",
        F.coalesce(F.last("rate_per_usd", True).over(fwd), F.first("rate_per_usd", True).over(bwd)).alias(
            "rate_per_usd"
        ),
        F.coalesce(F.last("q", True).over(fwd), F.first("q", True).over(bwd)).alias("fx_rate_date"),
    )


def with_usd(
    df: DataFrame, fx: DataFrame, date_col: str, currency_col: str, amount_cols: Sequence[str]
) -> DataFrame:
    """Join the daily FX rate of the transaction date and add ``<amount>_usd`` columns."""
    fx_ = fx.select(
        F.col("currency_code").alias("_fx_cur"),
        F.col("rate_date").alias("_fx_date"),
        F.col("rate_per_usd").alias("fx_rate_to_usd"),
        "fx_rate_date",
    )
    out = df.join(
        F.broadcast(fx_),
        (F.col(currency_col) == F.col("_fx_cur")) & (F.to_date(F.col(date_col)) == F.col("_fx_date")),
        "left",
    ).drop("_fx_cur", "_fx_date")
    for c in amount_cols:
        out = out.withColumn(f"{c}_usd", F.round(F.col(c) / F.col("fx_rate_to_usd"), 2))
    return out


# ------------------------------------------------------------- customers ---
def mask_email(col: str) -> Column:
    """j***@example.com"""
    return F.concat(F.substring(F.col(col), 1, 1), F.lit("***@"), F.substring_index(F.col(col), "@", -1))


def hash_pii(col: str, salt: str) -> Column:
    return F.sha2(F.concat(F.lit(salt), F.lower(F.trim(F.col(col)))), 256)


def mask_customers(df: DataFrame, salt: str) -> DataFrame:
    """Pseudonymise PII: hashed + masked e-mail, last name reduced to an initial."""
    return (
        df.withColumn("email_hash", hash_pii("email", salt))
        .withColumn("email_masked", mask_email("email"))
        .withColumn("last_name_initial", F.concat(F.substring("last_name", 1, 1), F.lit(".")))
        .withColumn("email_domain", F.substring_index(F.col("email"), "@", -1))
        .drop("email", "last_name")
    )


# --------------------------------------------------------------- folding ---
def fold_orders(events: DataFrame) -> DataFrame:
    """Order events -> one row per order with lifecycle timestamps (order-independent)."""
    e = events.withColumn("status_rank", rank_expr("order_status", ORDER_STATUS_RANK))

    def at(status: str) -> Column:
        return F.min(F.when(F.col("order_status") == status, F.col("event_timestamp"))).alias(f"{status}_at")

    return (
        e.groupBy("order_id")
        .agg(
            F.first("customer_id", True).alias("customer_id"),
            F.min("order_timestamp").alias("order_timestamp"),
            F.max("status_rank").alias("status_rank"),
            F.max_by("status_reason", "status_rank").alias("status_reason"),
            F.first("sales_channel", True).alias("sales_channel"),
            F.first("currency", True).alias("currency"),
            F.first("subtotal_amount", True).alias("subtotal_amount"),
            F.first("discount_amount", True).alias("discount_amount"),
            F.first("shipping_amount", True).alias("shipping_amount"),
            F.first("tax_amount", True).alias("tax_amount"),
            F.first("total_amount", True).alias("total_amount"),
            F.first("promotion_id", True).alias("promotion_id"),
            F.first("country", True).alias("country"),
            F.first("session_id", True).alias("session_id"),
            F.first("item_count", True).alias("item_count"),
            at("created"),
            at("confirmed"),
            at("cancelled"),
            at("shipped"),
            at("delivered"),
            at("returned"),
            F.max("event_timestamp").alias("last_event_at"),
            F.max(F.col("is_late_arrival").cast("int")).cast("boolean").alias("is_late_arrival"),
            F.count("*").alias("event_count"),
        )
        .withColumn("order_status", status_for_rank("status_rank", ORDER_STATUS_RANK))
    )


def fold_payments(events: DataFrame) -> DataFrame:
    e = events.withColumn("status_rank", rank_expr("payment_status", PAYMENT_STATUS_RANK))
    return e.groupBy("payment_id").agg(
        F.first("order_id", True).alias("order_id"),
        F.first("payment_method", True).alias("payment_method"),
        F.max("status_rank").alias("status_rank"),
        # authorized(0) < captured/failed(1); a payment is never both captured and failed.
        F.max_by("payment_status", "status_rank").alias("payment_status"),
        F.first("payment_amount", True).alias("payment_amount"),
        F.first("currency", True).alias("currency"),
        F.min("payment_timestamp").alias("payment_timestamp"),
        F.min(F.when(F.col("payment_status") == "authorized", F.col("event_timestamp"))).alias(
            "authorized_at"
        ),
        F.min(F.when(F.col("payment_status") == "captured", F.col("event_timestamp"))).alias("captured_at"),
        F.min(F.when(F.col("payment_status") == "failed", F.col("event_timestamp"))).alias("failed_at"),
        F.max("attempt_number").alias("attempt_number"),
        F.first("failure_reason", True).alias("failure_reason"),
        F.first("provider", True).alias("provider"),
        F.max("event_timestamp").alias("last_event_at"),
        F.max(F.col("is_late_arrival").cast("int")).cast("boolean").alias("is_late_arrival"),
    )


def fold_shipments(events: DataFrame) -> DataFrame:
    e = events.withColumn("status_rank", rank_expr("shipment_status", SHIPMENT_STATUS_RANK))
    return (
        e.groupBy("shipment_id")
        .agg(
            F.first("order_id", True).alias("order_id"),
            F.first("warehouse_id", True).alias("warehouse_id"),
            F.first("carrier", True).alias("carrier"),
            F.max("status_rank").alias("status_rank"),
            F.first("tracking_number", True).alias("tracking_number"),
            F.first("destination_country", True).alias("destination_country"),
            F.min(F.when(F.col("shipment_status") == "created", F.col("event_timestamp"))).alias(
                "created_at"
            ),
            F.min("shipped_at").alias("shipped_at"),
            F.max("estimated_delivery").alias("estimated_delivery"),
            F.max("actual_delivery").alias("actual_delivery"),
            F.max((F.col("shipment_status") == "delayed").cast("int"))
            .cast("boolean")
            .alias("had_delay_event"),
            F.max("event_timestamp").alias("last_event_at"),
            F.max(F.col("is_late_arrival").cast("int")).cast("boolean").alias("is_late_arrival"),
        )
        .withColumn("shipment_status", status_for_rank("status_rank", SHIPMENT_STATUS_RANK))
    )


def fold_returns(events: DataFrame) -> DataFrame:
    e = events.withColumn("status_rank", rank_expr("return_status", RETURN_STATUS_RANK))
    return (
        e.groupBy("return_id")
        .agg(
            F.first("order_id", True).alias("order_id"),
            F.first("order_item_id", True).alias("order_item_id"),
            F.first("product_id", True).alias("product_id"),
            F.first("quantity", True).alias("quantity"),
            F.first("return_reason", True).alias("return_reason"),
            F.first("warehouse_id", True).alias("warehouse_id"),
            F.min("requested_at").alias("requested_at"),
            F.max("received_at").alias("received_at"),
            F.max("status_rank").alias("status_rank"),
            F.max("event_timestamp").alias("last_event_at"),
        )
        .withColumn("return_status", status_for_rank("status_rank", RETURN_STATUS_RANK))
    )


# MERGE expressions that combine an existing folded row (t) with a new partial fold (s).
def lifecycle_merge_set(
    rank_col: str,
    status_col: str,
    timestamp_cols: Sequence[str],
    max_cols: Sequence[str] = (),
    immutable_cols: Sequence[str] = (),
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    newer = f"s.{rank_col} >= t.{rank_col}"
    sets = {
        rank_col: f"greatest(t.{rank_col}, s.{rank_col})",
        status_col: f"CASE WHEN {newer} THEN s.{status_col} ELSE t.{status_col} END",
        "last_event_at": "greatest(t.last_event_at, s.last_event_at)",
        "_silver_updated_at": "current_timestamp()",
    }
    for c in timestamp_cols:
        sets[c] = f"least(t.{c}, s.{c})"
    for c in max_cols:
        sets[c] = f"greatest(t.{c}, s.{c})"
    for c in immutable_cols:
        sets[c] = f"coalesce(t.{c}, s.{c})"
    sets.update(extra or {})
    return sets


# --------------------------------------------------------------- sessions ---
SESSION_GAP_SECONDS = 30 * 60


def reconstruct_sessions(clicks: DataFrame) -> DataFrame:
    """Rebuild sessions from clickstream events.

    * Client session ids are split again after 30 minutes of inactivity.
    * Identity stitching: if the visitor logs in mid-session, the whole session
      is attributed to that customer.
    * Funnel flags are derived from the event types in the session.
    """
    w = Window.partitionBy("session_id").orderBy("event_timestamp", "event_id")
    seq = (
        clicks.withColumn("_prev", F.lag("event_timestamp").over(w))
        .withColumn(
            "_new",
            F.when(
                F.col("_prev").isNull()
                | (F.col("event_timestamp").cast("long") - F.col("_prev").cast("long") > SESSION_GAP_SECONDS),
                1,
            ).otherwise(0),
        )
        .withColumn("_part", F.sum("_new").over(w))
    )
    return (
        seq.groupBy("session_id", "_part")
        .agg(
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.max("customer_id").alias("customer_id"),
            F.first("anonymous_id", True).alias("anonymous_id"),
            F.first("device_type", True).alias("device_type"),
            F.first("browser", True).alias("browser"),
            F.first("traffic_source", True).alias("traffic_source"),
            F.first("campaign_id", True).alias("campaign_id"),
            F.first("country", True).alias("country"),
            F.first("sales_channel", True).alias("sales_channel"),
            F.count("*").alias("event_count"),
            F.sum((F.col("event_type") == "page_view").cast("int")).alias("page_views"),
            F.sum((F.col("event_type") == "search").cast("int")).alias("searches"),
            F.sum((F.col("event_type") == "product_view").cast("int")).alias("product_views"),
            F.sum((F.col("event_type") == "add_to_cart").cast("int")).alias("add_to_cart_events"),
            F.sum((F.col("event_type") == "remove_from_cart").cast("int")).alias("remove_from_cart_events"),
            F.max((F.col("event_type") == "checkout_started").cast("int"))
            .cast("boolean")
            .alias("reached_checkout"),
            F.max((F.col("event_type") == "purchase").cast("int")).cast("boolean").alias("converted"),
        )
        .select(
            F.when(F.col("_part") == 1, F.col("session_id"))
            .otherwise(F.concat_ws("-", "session_id", F.col("_part").cast("string")))
            .alias("session_key"),
            "*",
        )
        .drop("_part")
        .withColumns(
            {
                "session_duration_seconds": F.col("session_end").cast("long")
                - F.col("session_start").cast("long"),
                "is_bounce": F.col("event_count") == 1,
                "has_cart": F.col("add_to_cart_events") > 0,
                "is_cart_abandoned": (F.col("add_to_cart_events") > 0) & ~F.col("converted"),
                "is_identified": F.col("customer_id").isNotNull(),
                "session_date": F.to_date("session_start"),
            }
        )
    )
