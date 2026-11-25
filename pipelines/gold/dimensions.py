"""Gold dimensions (Kimball). See architecture/data-model.md.

* Surrogate keys: ``xxhash64(natural_key, valid_from)``, deterministic across rebuilds.
* Every dimension has an unknown member (sk = -1).
* SCD1: overwrite-in-place dimensions (category, supplier, warehouse, promotion, country, currency, channel).
* SCD2:
    - ``dim_customer`` is derived from ``silver.customers_history``, which holds every
      tracked-attribute change. Versions are fully reconstructed from it.
    - ``dim_product`` uses the classic Delta MERGE pattern: changed rows close the
      current version (``valid_to``, ``is_current = false``) and insert a new one.
* Inferred members: natural keys referenced by facts but absent from the source
  are inserted with ``is_inferred = true`` and repaired by the next load (Type 1).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipelines.common.logs import get_logger
from pipelines.common.tables import delta_table, read_table, table, table_exists

log = get_logger("gold-dimensions")

HIGH_DATE = "9999-12-31 00:00:00"
LOW_DATE = "1900-01-01 00:00:00"
SALES_CHANNELS = [
    ("web", "Web storefront", "owned", "online"),
    ("mobile_app", "Mobile app", "owned", "online"),
    ("marketplace", "Marketplace partners", "third_party", "online"),
    ("social_commerce", "Social commerce", "third_party", "online"),
]


def sk(*cols: str) -> F.Column:
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])


def overwrite(df: DataFrame, fqn: str, partition_by: list[str] | None = None) -> int:
    ref = table(fqn)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(ref.qualified_name) if ref.uses_catalog else writer.save(ref.path)
    return df.count()


def with_unknown(spark: SparkSession, df: DataFrame, sk_col: str, nk_col: str) -> DataFrame:
    """Append the unknown member: sk -1, natural key 'UNKNOWN', other columns NULL / 'Unknown'."""
    values = []
    for f in df.schema.fields:
        if f.name == sk_col:
            values.append(F.lit(-1).cast("long").alias(f.name))
        elif f.name == nk_col:
            values.append(F.lit("UNKNOWN").alias(f.name))
        elif f.dataType.simpleString() == "string":
            values.append(F.lit("Unknown").alias(f.name))
        else:
            values.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.unionByName(spark.range(1).select(*values))


# ------------------------------------------------------------------ date --
def build_dim_date(spark: SparkSession) -> int:
    days = spark.range(1).select(
        F.explode(F.sequence(F.lit("2020-01-01").cast("date"), F.lit("2030-12-31").cast("date"))).alias(
            "date"
        )
    )
    # Black Friday = 4th Friday of November.
    nov1 = F.make_date(F.year("date"), F.lit(11), F.lit(1))
    first_fri = F.date_add(nov1, (F.lit(13) - F.dayofweek(nov1)) % 7)
    bf = F.date_add(first_fri, 21)
    df = days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofmonth("date").alias("day_of_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        (F.col("date") == bf).alias("is_black_friday"),
        (F.col("date") == F.date_add(bf, 3)).alias("is_cyber_monday"),
        ((F.month("date") == 12) & (F.dayofmonth("date") <= 24)).alias("is_holiday_season"),
        F.trunc("date", "month").alias("month_start"),
        F.date_trunc("week", "date").cast("date").alias("week_start"),
    )
    return overwrite(df, "gold.dim_date")


# ------------------------------------------------------------------ SCD1 --
def build_scd1_dimensions(spark: SparkSession) -> dict[str, int]:
    out: dict[str, int] = {}
    countries = read_table(spark, "silver.countries").select(
        sk("country_code").alias("country_sk"),
        "country_code",
        "country_name",
        "region",
        "currency_code",
        "vat_rate",
        "timezone",
    )
    out["dim_country"] = overwrite(
        with_unknown(spark, countries, "country_sk", "country_code"), "gold.dim_country"
    )

    currencies = read_table(spark, "silver.currencies").select(
        sk("currency_code").alias("currency_sk"), "currency_code", "currency_name", "minor_units"
    )
    out["dim_currency"] = overwrite(
        with_unknown(spark, currencies, "currency_sk", "currency_code"), "gold.dim_currency"
    )

    channels = spark.createDataFrame(
        SALES_CHANNELS,
        "channel_code STRING, channel_name STRING, channel_ownership STRING, channel_type STRING",
    )
    channels = channels.select(sk("channel_code").alias("channel_sk"), "*")
    out["dim_sales_channel"] = overwrite(
        with_unknown(spark, channels, "channel_sk", "channel_code"), "gold.dim_sales_channel"
    )

    cats = read_table(spark, "silver.categories").select(
        sk("category_id").alias("category_sk"), "category_id", "category_name", "department"
    )
    out["dim_category"] = overwrite(
        with_unknown(spark, cats, "category_sk", "category_id"), "gold.dim_category"
    )

    sups = read_table(spark, "silver.suppliers").select(
        sk("supplier_id").alias("supplier_sk"),
        "supplier_id",
        "supplier_name",
        "country_code",
        "lead_time_days",
        "reliability_score",
        "is_active",
    )
    out["dim_supplier"] = overwrite(
        with_unknown(spark, sups, "supplier_sk", "supplier_id"), "gold.dim_supplier"
    )

    whs = (
        read_table(spark, "silver.warehouses")
        .join(read_table(spark, "silver.countries").select("country_code", "region"), "country_code", "left")
        .select(
            sk("warehouse_id").alias("warehouse_sk"),
            "warehouse_id",
            "warehouse_name",
            "country_code",
            "city",
            "region",
            "capacity_units",
        )
    )
    out["dim_warehouse"] = overwrite(
        with_unknown(spark, whs, "warehouse_sk", "warehouse_id"), "gold.dim_warehouse"
    )

    promos = read_table(spark, "silver.promotions").select(
        sk("promotion_id").alias("promotion_sk"),
        "promotion_id",
        "promotion_name",
        "promotion_type",
        "discount_pct",
        "start_date",
        "end_date",
        "channel",
        "department",
    )
    promos = with_unknown(spark, promos, "promotion_sk", "promotion_id")
    # A second special member: orders without any promotion.
    none_row = spark.range(1).select(
        F.lit(-2).cast("long").alias("promotion_sk"),
        F.lit("NONE").alias("promotion_id"),
        F.lit("No promotion").alias("promotion_name"),
        F.lit("none").alias("promotion_type"),
        F.lit(0.0).alias("discount_pct"),
        F.lit(None).cast("date").alias("start_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.lit(None).cast("string").alias("channel"),
        F.lit(None).cast("string").alias("department"),
    )
    out["dim_promotion"] = overwrite(promos.unionByName(none_row), "gold.dim_promotion")
    return out


# -------------------------------------------------------- customer SCD2 --
CUSTOMER_SCD2 = ["country_code", "customer_segment", "loyalty_status"]


def build_dim_customer(spark: SparkSession) -> int:
    """SCD2 versions from the attribute history; SCD1 attributes from the current Silver row.

    The first version is back-dated to the registration date, so orders placed
    before the first recorded change still find a matching version.
    """
    hist = read_table(spark, "silver.customers_history")
    current = read_table(spark, "silver.customers").select(
        "customer_id",
        "first_name",
        "last_name_initial",
        "email_masked",
        "email_hash",
        "email_domain",
        "registration_date",
        "marketing_opt_in",
    )
    w = Window.partitionBy("customer_id").orderBy("valid_from")
    versions = (
        hist.withColumn("_n", F.row_number().over(w))
        .withColumn("_next", F.lead("valid_from").over(w))
        .join(current, "customer_id", "left")
        .withColumn(
            "valid_from",
            F.when(
                F.col("_n") == 1,
                F.least(
                    "valid_from",
                    F.coalesce("registration_date", "valid_from"),
                    F.lit(LOW_DATE).cast("timestamp"),
                ),
            ).otherwise(F.col("valid_from")),
        )
        .withColumn("valid_to", F.coalesce("_next", F.lit(HIGH_DATE).cast("timestamp")))
        .withColumn("is_current", F.col("_next").isNull())
        .withColumn("version", F.col("_n"))
        .withColumn("is_inferred", F.lit(False))
    )
    dim = versions.select(
        sk("customer_id", "valid_from").alias("customer_sk"),
        "customer_id",
        "first_name",
        "last_name_initial",
        "email_masked",
        "email_hash",
        "email_domain",
        "registration_date",
        "marketing_opt_in",
        *CUSTOMER_SCD2,
        "valid_from",
        "valid_to",
        "is_current",
        "version",
        "is_inferred",
    )
    # Inferred members: customers referenced by orders but unknown to the master data (late-arriving dimension).
    known = dim.select("customer_id").distinct()
    orphans = (
        read_table(spark, "silver.orders")
        .select("customer_id")
        .distinct()
        .join(known, "customer_id", "left_anti")
        .filter(F.col("customer_id").isNotNull())
    )
    inferred = orphans.select(
        F.xxhash64(F.col("customer_id"), F.lit(LOW_DATE)).alias("customer_sk"),
        "customer_id",
        *[
            F.lit(None).cast(t).alias(c)
            for c, t in [
                ("first_name", "string"),
                ("last_name_initial", "string"),
                ("email_masked", "string"),
                ("email_hash", "string"),
                ("email_domain", "string"),
                ("registration_date", "timestamp"),
                ("marketing_opt_in", "boolean"),
            ]
        ],
        F.lit("Unknown").alias("country_code"),
        F.lit("Unknown").alias("customer_segment"),
        F.lit("Unknown").alias("loyalty_status"),
        F.lit(LOW_DATE).cast("timestamp").alias("valid_from"),
        F.lit(HIGH_DATE).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.lit(1).alias("version"),
        F.lit(True).alias("is_inferred"),
    )
    full = dim.unionByName(inferred)
    full = with_unknown(spark, full, "customer_sk", "customer_id")
    n = overwrite(full, "gold.dim_customer")
    log.info("dim_customer built", extra={"rows": n, "inferred": orphans.count()})
    return n


# --------------------------------------------------------- product SCD2 --
PRODUCT_SCD2 = ["unit_price", "product_status", "category_id", "supplier_id"]
PRODUCT_SCD1 = [
    "product_name",
    "brand",
    "unit_cost",
    "weight_kg",
    "category_name",
    "department",
    "supplier_name",
    "gross_margin_pct",
]


def build_dim_product(spark: SparkSession) -> dict[str, int]:
    """SCD2 via MERGE: close changed current rows, insert their new versions and brand-new products."""
    src = read_table(spark, "silver.products").select(
        "product_id", *PRODUCT_SCD2, *PRODUCT_SCD1, F.col("updated_at").alias("change_ts")
    )
    src = src.withColumn(
        "_hash",
        F.sha2(
            F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in PRODUCT_SCD2]), 256
        ),
    )
    ref = table("gold.dim_product")
    if not table_exists(spark, ref):
        first = src.select(
            F.xxhash64("product_id", F.lit(LOW_DATE)).alias("product_sk"),
            "product_id",
            *PRODUCT_SCD2,
            *PRODUCT_SCD1,
            "_hash",
            F.lit(LOW_DATE).cast("timestamp").alias("valid_from"),
            F.lit(HIGH_DATE).cast("timestamp").alias("valid_to"),
            F.lit(True).alias("is_current"),
            F.lit(False).alias("is_inferred"),
        )
        overwrite(with_unknown(spark, first, "product_sk", "product_id"), "gold.dim_product")
        return {"inserted": first.count(), "closed": 0}

    current = (
        read_table(spark, ref)
        .filter("is_current AND product_id <> 'UNKNOWN'")
        .select("product_id", F.col("_hash").alias("_cur_hash"))
    )
    changed = (
        src.join(current, "product_id", "inner")
        .filter(F.col("_hash") != F.col("_cur_hash"))
        .drop("_cur_hash")
    )
    new = src.join(current, "product_id", "left_anti")
    # Materialise before the MERGE: afterwards the comparison would see the new versions as current.
    changed, new = changed.cache(), new.cache()
    result = {"closed_versions": changed.count(), "new_products": new.count()}
    # Staging: changed rows twice (one closes the old version via merge key, one inserts with NULL key).
    staged = (
        changed.withColumn("_merge_key", F.col("product_id"))
        .unionByName(changed.withColumn("_merge_key", F.lit(None).cast("string")))
        .unionByName(new.withColumn("_merge_key", F.lit(None).cast("string")))
    )
    insert_values = {
        "product_sk": "xxhash64(s.product_id, cast(s.change_ts as string))",
        "product_id": "s.product_id",
        **{c: f"s.{c}" for c in PRODUCT_SCD2 + PRODUCT_SCD1},
        "_hash": "s._hash",
        "valid_from": "s.change_ts",
        "valid_to": f"timestamp'{HIGH_DATE}'",
        "is_current": "true",
        "is_inferred": "false",
    }
    (
        delta_table(spark, ref)
        .alias("t")
        .merge(staged.alias("s"), "t.product_id = s._merge_key AND t.is_current")
        .whenMatchedUpdate(
            condition="t._hash <> s._hash", set={"valid_to": "s.change_ts", "is_current": "false"}
        )
        .whenNotMatchedInsert(condition="s._merge_key IS NULL", values=insert_values)
        .execute()
    )
    # SCD1 attributes are corrected on every version (e.g. a product rename).
    scd1 = src.select("product_id", *PRODUCT_SCD1)
    (
        delta_table(spark, ref)
        .alias("t")
        .merge(scd1.alias("s"), "t.product_id = s.product_id")
        .whenMatchedUpdate(set={c: f"s.`{c}`" for c in PRODUCT_SCD1})
        .execute()
    )
    changed.unpersist()
    new.unpersist()
    log.info("dim_product SCD2 merge", extra=result)
    return result


def build_all_dimensions(spark: SparkSession) -> dict[str, object]:
    out: dict[str, object] = {"dim_date": build_dim_date(spark)}
    out.update(build_scd1_dimensions(spark))
    out["dim_customer"] = build_dim_customer(spark)
    out["dim_product"] = build_dim_product(spark)
    return out
