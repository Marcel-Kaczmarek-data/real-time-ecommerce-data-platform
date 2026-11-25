# Data quality report

- Run id: `dq-20261125T022344`
- Executed at (UTC): 2026-11-25 02:23:58
- Checks: 63 (pass 60, fail 3, error 0)
- Severity-weighted quality score: **96.2 / 100**

| Status | Severity | Suite | Check | Table | Failed | Rate | Threshold | Details |
|---|---|---|---|---|---:|---:|---:|---|
| ❌ FAIL | warning | bronze | `bronze_clickstream_freshness` | `bronze.clickstream_events` | 1 | 100.0000% | 0.00% | newest ingestion_timestamp = 2026-11-24 19:26 (7.0 h old, max 2.0 h) |
| ❌ FAIL | warning | bronze | `bronze_orders_freshness` | `bronze.orders_events` | 1 | 100.0000% | 0.00% | newest ingestion_timestamp = 2026-11-24 21:49 (4.6 h old, max 2.0 h) |
| ❌ FAIL | warning | silver | `inventory_negative_stock` | `silver.inventory` | 18 | 0.3545% | 0.00% | rows violating: available_quantity >= 0 AND reserved_quantity >= 0 |
| ✅ PASS | critical | bronze | `bronze_orders_envelope_complete` | `bronze.orders_events` | 0 | 0.0000% | 0.00% | rows with NULL in any of ['event_id', 'event_type', 'event_timestamp', 'raw_payload', 'kafka_offset'] |
| ✅ PASS | critical | bronze | `bronze_orders_kafka_offsets_unique` | `bronze.orders_events` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['kafka_partition', 'kafka_offset'] |
| ✅ PASS | critical | silver | `clickstream_event_type_accepted` | `silver.clickstream` | 0 | 0.0000% | 0.00% | unexpected values (sample): [] |
| ✅ PASS | critical | silver | `clickstream_pk_unique` | `silver.clickstream` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['event_id'] |
| ✅ PASS | critical | gold | `customer_360_one_row_per_customer` | `gold.mart_customer_360` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['customer_id'] |
| ✅ PASS | critical | silver | `customers_pii_masked` | `silver.customers` | 0 | 0.0000% | 0.00% | rows violating: email_masked LIKE '%***@%' AND length(email_hash) = 64 |
| ✅ PASS | critical | silver | `customers_pk_unique` | `silver.customers` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['customer_id'] |
| ✅ PASS | critical | gold | `dim_customer_one_current_version` | `gold.dim_customer` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | critical | gold | `dim_customer_versions_do_not_overlap` | `gold.dim_customer` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | critical | gold | `dim_product_one_current_version` | `gold.dim_product` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | critical | gold | `fact_order_items_grain_unique` | `gold.fact_order_items` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['order_item_id'] |
| ✅ PASS | critical | gold | `fact_orders_grain_unique` | `gold.fact_orders` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['order_id'] |
| ✅ PASS | critical | silver | `fx_rates_cover_currencies` | `silver.exchange_rates` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | critical | silver | `inventory_movements_pk_unique` | `silver.inventory_movements` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['movement_id'] |
| ✅ PASS | critical | gold | `net_revenue_not_above_gross` | `gold.fact_orders` | 0 | 0.0000% | 0.00% | rows violating: net_revenue_usd <= gross_revenue_usd + 0.01 |
| ✅ PASS | critical | silver | `order_items_pk_unique` | `silver.order_items` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['order_item_id'] |
| ✅ PASS | critical | silver | `order_items_quantity_range` | `silver.order_items` | 0 | 0.0000% | 0.00% | observed min=1 max=6, allowed [1.0, 100.0] |
| ✅ PASS | critical | silver | `orders_amounts_non_negative` | `silver.orders` | 0 | 0.0000% | 0.00% | rows violating: subtotal_amount >= 0 AND discount_amount >= 0 AND total_amount >= 0 |
| ✅ PASS | critical | silver | `orders_mandatory_fields` | `silver.orders` | 0 | 0.0000% | 0.00% | rows with NULL in any of ['order_id', 'customer_id', 'order_timestamp', 'order_status', 'currency', 'total_amount', 'total_amount_usd'] |
| ✅ PASS | critical | silver | `orders_pk_unique` | `silver.orders` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['order_id'] |
| ✅ PASS | critical | silver | `orders_status_accepted` | `silver.orders` | 0 | 0.0000% | 0.00% | unexpected values (sample): [] |
| ✅ PASS | critical | silver | `orders_timestamp_valid` | `silver.orders` | 0 | 0.0000% | 0.00% | rows violating: order_timestamp BETWEEN timestamp'2020-01-01' AND current_timestamp() + INTERVAL 5 MINUTES |
| ✅ PASS | critical | silver | `orders_total_reconciles` | `silver.orders` | 0 | 0.0000% | 0.00% | rows violating: abs(total_amount - (subtotal_amount - discount_amount + shipping_amount + tax_amount)) <= 0.05 |
| ✅ PASS | critical | silver | `payments_amount_positive` | `silver.payments` | 0 | 0.0000% | 0.00% | observed min=7.15 max=4012701.0, allowed [0.01, None] |
| ✅ PASS | critical | silver | `payments_pk_unique` | `silver.payments` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['payment_id'] |
| ✅ PASS | critical | silver | `payments_single_capture_per_order` | `silver.payments` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | critical | silver | `payments_status_accepted` | `silver.payments` | 0 | 0.0000% | 0.00% | unexpected values (sample): [] |
| ✅ PASS | critical | silver | `products_pk_unique` | `silver.products` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['product_id'] |
| ✅ PASS | critical | silver | `products_price_positive` | `silver.products` | 0 | 0.0000% | 0.00% | observed min=2.0 max=1698.49, allowed [0.01, None] |
| ✅ PASS | critical | silver | `refunds_not_exceeding_order` | `silver.refunds` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | critical | silver | `refunds_pk_unique` | `silver.refunds` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['refund_id'] |
| ✅ PASS | critical | silver | `returns_pk_unique` | `silver.returns` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['return_id'] |
| ✅ PASS | critical | gold | `revenue_reconciles_with_facts` | `gold.mart_daily_sales` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | critical | silver | `shipments_delivery_after_shipping` | `silver.shipments` | 0 | 0.0000% | 0.00% | rows violating: actual_delivery IS NULL OR actual_delivery >= shipped_at |
| ✅ PASS | critical | silver | `shipments_pk_unique` | `silver.shipments` | 0 | 0.0000% | 0.00% | 0 duplicate key rows on ['shipment_id'] |
| ✅ PASS | warning | bronze | `bronze_clickstream_duplicate_event_rate` | `bronze.clickstream_events` | 592 | 0.1946% | 1.00% | 592 duplicate events (Bronze keeps them; Silver deduplicates) |
| ✅ PASS | warning | bronze | `bronze_corrupt_events` | `quarantine.bronze_corrupt_events` | 0 | 0.0000% | 0.00% | 0 rows; within expected bounds |
| ✅ PASS | warning | bronze | `bronze_orders_duplicate_event_rate` | `bronze.orders_events` | 24 | 0.2649% | 1.00% | 24 duplicate events (Bronze keeps them; Silver deduplicates) |
| ✅ PASS | warning | bronze | `bronze_payments_duplicate_event_rate` | `bronze.payments_events` | 11 | 0.2313% | 1.00% | 11 duplicate events (Bronze keeps them; Silver deduplicates) |
| ✅ PASS | warning | silver | `customers_country_exists` | `silver.customers` | 0 | 0.0000% | 0.00% | silver.customers.country_code values missing in silver.countries.country_code |
| ✅ PASS | warning | gold | `executive_summary_freshness` | `gold.mart_executive_summary` | 0 | 0.0000% | 0.00% | newest date = 2026-11-25 00:00 (2.4 h old, max 48.0 h) |
| ✅ PASS | warning | gold | `fact_order_items_product_sk_resolved` | `gold.fact_order_items` | 0 | 0.0000% | 0.10% | rows violating: product_sk <> -1 |
| ✅ PASS | warning | gold | `fact_orders_customer_sk_resolved` | `gold.fact_orders` | 0 | 0.0000% | 0.10% | rows violating: customer_sk <> -1 |
| ✅ PASS | warning | silver | `fx_rates_freshness` | `silver.exchange_rates` | 0 | 0.0000% | 0.00% | newest rate_date = 2026-11-24 00:00 (26.4 h old, max 72.0 h) |
| ✅ PASS | warning | silver | `order_items_currency_consistent` | `silver.order_items` | 0 | 0.0000% | 0.00% | 0 failing records from custom SQL |
| ✅ PASS | warning | silver | `order_items_order_exists` | `silver.order_items` | 18 | 0.0468% | 1.00% | silver.order_items.order_id values missing in silver.orders.order_id |
| ✅ PASS | warning | silver | `order_items_product_exists` | `silver.order_items` | 0 | 0.0000% | 0.00% | silver.order_items.product_id values missing in silver.products.product_id |
| ✅ PASS | warning | silver | `order_payment_reconciliation` | `silver.payments` | 0 | 0.0000% | 0.50% | 0 failing records from custom SQL |
| ✅ PASS | warning | silver | `orders_channel_accepted` | `silver.orders` | 0 | 0.0000% | 0.00% | unexpected values (sample): [] |
| ✅ PASS | warning | silver | `orders_currency_matches_country` | `silver.orders` | 0 | 0.0000% | 0.10% | 0 failing records from custom SQL |
| ✅ PASS | warning | silver | `orders_customer_exists` | `silver.orders` | 0 | 0.0000% | 1.00% | silver.orders.customer_id values missing in silver.customers.customer_id |
| ✅ PASS | warning | silver | `orders_freshness` | `silver.orders` | 0 | 0.0000% | 0.00% | newest last_event_at = 2026-11-24 21:44 (4.7 h old, max 6.0 h) |
| ✅ PASS | warning | silver | `orders_row_count_anomaly` | `silver.orders` | 0 | 0.0000% | 0.00% | 31814 rows; within expected bounds |
| ✅ PASS | warning | silver | `orders_valid_status_transitions` | `silver.order_status_history` | 3 | 0.0332% | 0.10% | 3 failing records from custom SQL |
| ✅ PASS | warning | silver | `orders_vs_items_subtotal` | `silver.orders` | 7 | 0.0220% | 0.50% | 7 failing records from custom SQL |
| ✅ PASS | warning | silver | `payments_order_exists` | `silver.payments` | 11 | 0.0338% | 1.00% | silver.payments.order_id values missing in silver.orders.order_id |
| ✅ PASS | warning | silver | `sessions_duration_non_negative` | `silver.customer_sessions` | 0 | 0.0000% | 0.00% | observed min=0 max=2211, allowed [0.0, None] |
| ✅ PASS | informational | silver | `clickstream_late_arrival_rate` | `silver.clickstream` | 842 | 0.2775% | 2.00% | rows violating: NOT is_late_arrival |
| ✅ PASS | informational | silver | `shipments_carrier_accepted` | `silver.shipments` | 0 | 0.0000% | 0.00% | unexpected values (sample): [] |
| ✅ PASS | informational | bronze | `silver_rejects_volume` | `quarantine.silver_rejects` | 0 | 0.0000% | 0.00% | 130 rows; within expected bounds |
