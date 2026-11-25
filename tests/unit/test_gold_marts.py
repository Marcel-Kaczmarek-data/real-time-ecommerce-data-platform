"""Gold correctness: no double counting across fact grains, SCD2 behaviour, unknown members."""

from __future__ import annotations

from datetime import date, datetime

import pytest

pytestmark = pytest.mark.spark

ORDER_TS = datetime(2026, 11, 10, 12, 0)


def _write(spark, fqn: str, rows: list[tuple], schema: str) -> None:  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    from pipelines.common.tables import table

    df = spark.createDataFrame(rows, schema).withColumn("_silver_updated_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table(fqn).path)


@pytest.fixture(scope="module")
def gold_world(spark, lakehouse_root):  # type: ignore[no-untyped-def]
    """Minimal Silver layer: 1 customer, 3 products, 1 order with 3 lines, 2 payment attempts, 2 refunds."""
    _write(
        spark,
        "silver.countries",
        [("DE", "Germany", "Europe", "EUR", 0.19, "Europe/Berlin")],
        "country_code STRING, country_name STRING, region STRING, currency_code STRING, vat_rate DOUBLE, "
        "timezone STRING",
    )
    _write(
        spark,
        "silver.currencies",
        [("EUR", "Euro", 2)],
        "currency_code STRING, currency_name STRING, minor_units INT",
    )
    _write(
        spark,
        "silver.categories",
        [("CAT-1", "Shoes", "Fashion", "Fashion"), ("CAT-2", "Books", "Media", "Media")],
        "category_id STRING, category_name STRING, department STRING, parent_category STRING",
    )
    _write(
        spark,
        "silver.suppliers",
        [("SUP-1", "Acme", "DE", 5, 0.95, True)],
        "supplier_id STRING, supplier_name STRING, country_code STRING, lead_time_days INT, "
        "reliability_score DOUBLE, is_active BOOLEAN",
    )
    _write(
        spark,
        "silver.warehouses",
        [("WH-1", "Leipzig FC", "DE", "Leipzig", 1000)],
        "warehouse_id STRING, warehouse_name STRING, country_code STRING, city STRING, capacity_units INT",
    )
    _write(
        spark,
        "silver.promotions",
        [],
        "promotion_id STRING, promotion_name STRING, promotion_type STRING, "
        "discount_pct DOUBLE, start_date DATE, end_date DATE, channel STRING, department STRING",
    )
    _write(
        spark,
        "silver.customers",
        [("C1", "Anna", "S.", "a***@example.com", "h", "example.com", datetime(2025, 1, 1), True)],
        "customer_id STRING, first_name STRING, last_name_initial STRING, email_masked STRING, "
        "email_hash STRING, email_domain STRING, registration_date TIMESTAMP, marketing_opt_in BOOLEAN",
    )
    _write(
        spark,
        "silver.customers_history",
        [
            ("C1", "DE", "consumer", "bronze", datetime(2025, 1, 1)),
            ("C1", "DE", "consumer", "gold", datetime(2026, 11, 20)),
        ],  # upgrade AFTER the order
        "customer_id STRING, country_code STRING, customer_segment STRING, loyalty_status STRING, "
        "valid_from TIMESTAMP",
    )
    products = [
        (
            f"P{i}",
            f"Product {i}",
            "CAT-1" if i < 3 else "CAT-2",
            "SUP-1",
            "Brand",
            50.0,
            20.0,
            "USD",
            "active",
            1.0,
            datetime(2026, 1, 1),
            "Shoes" if i < 3 else "Books",
            "Fashion",
            "Acme",
            0.6,
        )
        for i in (1, 2, 3)
    ]
    product_schema = (
        "product_id STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, "
        "unit_price DOUBLE, unit_cost DOUBLE, currency STRING, product_status STRING, weight_kg DOUBLE, "
        "updated_at TIMESTAMP, category_name STRING, department STRING, supplier_name STRING, "
        "gross_margin_pct DOUBLE"
    )
    _write(spark, "silver.products", products, product_schema)
    # Order: 3 lines (100 + 2x50 + 30 = 230 EUR subtotal), discount 20, shipping 0, tax 39.90 -> total 249.90.
    # FX: 1 USD = 0.5 EUR to make USD figures easy (x2).
    _write(
        spark,
        "silver.orders",
        [
            (
                "O1",
                "C1",
                ORDER_TS,
                4,
                "delivered",
                None,
                "web",
                "EUR",
                230.0,
                20.0,
                0.0,
                39.9,
                249.9,
                None,
                "DE",
                "S1",
                4,
                ORDER_TS,
                ORDER_TS,
                None,
                ORDER_TS,
                ORDER_TS,
                None,
                ORDER_TS,
                False,
                5,
                "kafka",
                0.5,
                date(2026, 11, 10),
                460.0,
                40.0,
                0.0,
                79.8,
                499.8,
            )
        ],
        "order_id STRING, customer_id STRING, order_timestamp TIMESTAMP, status_rank INT, order_status STRING, "
        "status_reason STRING, sales_channel STRING, currency STRING, subtotal_amount DOUBLE, discount_amount DOUBLE, "
        "shipping_amount DOUBLE, tax_amount DOUBLE, total_amount DOUBLE, promotion_id STRING, country STRING, "
        "session_id STRING, item_count INT, created_at TIMESTAMP, confirmed_at TIMESTAMP, cancelled_at TIMESTAMP, "
        "shipped_at TIMESTAMP, delivered_at TIMESTAMP, returned_at TIMESTAMP, last_event_at TIMESTAMP, "
        "is_late_arrival BOOLEAN, event_count INT, _source STRING, fx_rate_to_usd DOUBLE, fx_rate_date DATE, "
        "subtotal_amount_usd DOUBLE, discount_amount_usd DOUBLE, shipping_amount_usd DOUBLE, tax_amount_usd DOUBLE, "
        "total_amount_usd DOUBLE",
    )
    _write(
        spark,
        "silver.order_items",
        [
            ("OI1", "O1", "P1", 1, 100.0, 20.0, "EUR", 1, 100.0, 80.0, ORDER_TS, "kafka"),
            ("OI2", "O1", "P2", 2, 50.0, 0.0, "EUR", 2, 100.0, 100.0, ORDER_TS, "kafka"),
            ("OI3", "O1", "P3", 1, 30.0, 0.0, "EUR", 3, 30.0, 30.0, ORDER_TS, "kafka"),
        ],
        "order_item_id STRING, order_id STRING, product_id STRING, quantity INT, unit_price DOUBLE, "
        "discount_amount DOUBLE, currency STRING, line_number INT, line_gross_amount DOUBLE, line_net_amount DOUBLE, "
        "event_timestamp TIMESTAMP, _source STRING",
    )
    pay_schema = (
        "payment_id STRING, order_id STRING, payment_method STRING, status_rank INT, payment_status STRING, "
        "payment_amount DOUBLE, currency STRING, payment_timestamp TIMESTAMP, authorized_at TIMESTAMP, "
        "captured_at TIMESTAMP, failed_at TIMESTAMP, attempt_number INT, failure_reason STRING, "
        "provider STRING, last_event_at TIMESTAMP, is_late_arrival BOOLEAN, fx_rate_to_usd DOUBLE, "
        "fx_rate_date DATE, payment_amount_usd DOUBLE, _source STRING"
    )
    _write(
        spark,
        "silver.payments",
        [
            (
                "PAY1",
                "O1",
                "credit_card",
                1,
                "failed",
                249.9,
                "EUR",
                ORDER_TS,
                None,
                None,
                ORDER_TS,
                1,
                "card_declined",
                "stripe",
                ORDER_TS,
                False,
                0.5,
                date(2026, 11, 10),
                499.8,
                "kafka",
            ),
            (
                "PAY2",
                "O1",
                "paypal",
                1,
                "captured",
                249.9,
                "EUR",
                ORDER_TS,
                ORDER_TS,
                ORDER_TS,
                None,
                2,
                None,
                "paypal",
                ORDER_TS,
                False,
                0.5,
                date(2026, 11, 10),
                499.8,
                "kafka",
            ),
        ],
        pay_schema,
    )
    ref_schema = (
        "refund_id STRING, order_id STRING, payment_id STRING, return_id STRING, refund_amount DOUBLE, "
        "refund_tax_amount DOUBLE, currency STRING, refund_reason STRING, refund_status STRING, "
        "refund_timestamp TIMESTAMP, is_late_arrival BOOLEAN, fx_rate_to_usd DOUBLE, fx_rate_date DATE, "
        "refund_amount_usd DOUBLE, refund_tax_amount_usd DOUBLE, _source STRING"
    )
    _write(
        spark,
        "silver.refunds",
        [
            (
                "R1",
                "O1",
                "PAY2",
                "RET1",
                35.7,
                5.7,
                "EUR",
                "product_return",
                "completed",
                datetime(2026, 11, 15),
                False,
                0.5,
                date(2026, 11, 15),
                71.4,
                11.4,
                "kafka",
            ),
            (
                "R2",
                "O1",
                "PAY2",
                "RET2",
                11.9,
                1.9,
                "EUR",
                "product_return",
                "completed",
                datetime(2026, 11, 16),
                False,
                0.5,
                date(2026, 11, 16),
                23.8,
                3.8,
                "kafka",
            ),
        ],
        ref_schema,
    )

    from pipelines.gold.dimensions import build_all_dimensions
    from pipelines.gold.facts import load_fact_order_items, load_fact_orders

    build_all_dimensions(spark)
    load_fact_orders(spark)
    load_fact_order_items(spark)
    return spark


def test_no_double_counting(gold_world) -> None:  # type: ignore[no-untyped-def]
    from pipelines.common.tables import read_table
    from pipelines.gold.marts import build_marts

    spark = gold_world
    fo = read_table(spark, "gold.fact_orders").collect()
    assert len(fo) == 1, "one row per order regardless of 3 lines x 2 payments x 2 refunds"
    o = fo[0]
    assert o.is_paid and o.payment_attempts == 2 and o.failed_payment_attempts == 1
    # Gross revenue = (subtotal - discount + shipping) in USD = (460 - 40 + 0) = 420.
    assert o.gross_revenue_usd == pytest.approx(420.0)
    # Refunds ex tax = (71.4 - 11.4) + (23.8 - 3.8) = 80 -> net revenue 340.
    assert o.refund_amount_usd == pytest.approx(95.2)
    assert o.net_revenue_usd == pytest.approx(340.0)
    assert o.captured_amount_usd == pytest.approx(499.8)  # the failed attempt is not counted

    build_marts(spark, ["mart_daily_sales", "mart_daily_category_sales"])
    ds = read_table(spark, "gold.mart_daily_sales").collect()
    assert len(ds) == 1
    assert ds[0].paid_orders == 1 and ds[0].placed_orders == 1
    assert ds[0].gross_revenue_usd == pytest.approx(420.0)
    assert ds[0].net_revenue_usd == pytest.approx(340.0)
    assert ds[0].aov_usd == pytest.approx(420.0)
    cats = {r.category_name: r for r in read_table(spark, "gold.mart_daily_category_sales").collect()}
    # Lines: P1 net 80 EUR + P2 100 EUR (Shoes) = 180 EUR = 360 USD; P3 30 EUR (Books) = 60 USD.
    assert cats["Shoes"].net_merchandise_usd == pytest.approx(360.0)
    assert cats["Books"].net_merchandise_usd == pytest.approx(60.0)
    assert sum(c.net_merchandise_usd for c in cats.values()) == pytest.approx(
        o.subtotal_amount_usd - o.discount_amount_usd
    )


def test_fact_uses_customer_version_valid_at_order_time(gold_world) -> None:  # type: ignore[no-untyped-def]
    from pipelines.common.tables import read_table

    spark = gold_world
    dim = {r.customer_sk: r for r in read_table(spark, "gold.dim_customer").collect()}
    o = read_table(spark, "gold.fact_orders").collect()[0]
    version = dim[o.customer_sk]
    assert version.loyalty_status == "bronze"  # the gold upgrade happened after the order
    assert not version.is_current
    current = [r for r in dim.values() if r.customer_id == "C1" and r.is_current]
    assert len(current) == 1 and current[0].loyalty_status == "gold"
    assert -1 in dim and dim[-1].customer_id == "UNKNOWN"


def test_dim_product_scd2_merge_closes_and_inserts_versions(gold_world) -> None:  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    from pipelines.common.tables import read_table, table
    from pipelines.gold.dimensions import build_dim_product

    spark = gold_world
    products = read_table(spark, "silver.products")
    changed = products.withColumn(
        "unit_price", F.when(F.col("product_id") == "P1", F.lit(60.0)).otherwise(F.col("unit_price"))
    ).withColumn(
        "updated_at",
        F.when(F.col("product_id") == "P1", F.lit(datetime(2026, 11, 21))).otherwise(F.col("updated_at")),
    )
    changed.write.format("delta").mode("overwrite").save(table("silver.products").path)
    result = build_dim_product(spark)
    assert result == {"closed_versions": 1, "new_products": 0}
    rows = [r for r in read_table(spark, "gold.dim_product").collect() if r.product_id == "P1"]
    assert len(rows) == 2
    old = next(r for r in rows if not r.is_current)
    new = next(r for r in rows if r.is_current)
    assert old.unit_price == 50.0 and new.unit_price == 60.0
    assert old.valid_to == new.valid_from == datetime(2026, 11, 21)
    # Re-running with no source change is a no-op.
    assert build_dim_product(spark) == {"closed_versions": 0, "new_products": 0}
