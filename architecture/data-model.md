# Dimensional Data Model (Gold)

The Gold layer is a **Kimball star schema** with conformed dimensions, plus
purpose-built marts. All monetary facts carry the **transaction currency
amount** and the **reporting-currency (USD) amount**. The USD amount uses the
daily FX rate of the transaction date. If no rate exists for that date, the
latest earlier rate is used (`fx_rate_date` is stored for audit).

## Star schema

```mermaid
erDiagram
    dim_date ||--o{ fact_orders : order_date_key
    dim_customer ||--o{ fact_orders : customer_sk
    dim_country ||--o{ fact_orders : country_sk
    dim_currency ||--o{ fact_orders : currency_sk
    dim_sales_channel ||--o{ fact_orders : channel_sk
    dim_promotion ||--o{ fact_orders : promotion_sk

    fact_orders ||--o{ fact_order_items : order_id
    dim_product ||--o{ fact_order_items : product_sk
    dim_category ||--o{ dim_product : category_sk
    dim_supplier ||--o{ dim_product : supplier_sk

    fact_orders ||--o{ fact_payments : order_id
    fact_orders ||--o{ fact_shipments : order_id
    dim_warehouse ||--o{ fact_shipments : warehouse_sk
    fact_orders ||--o{ fact_refunds : order_id
    fact_order_items ||--o{ fact_returns : order_item_id

    dim_product ||--o{ fact_inventory_snapshots : product_sk
    dim_warehouse ||--o{ fact_inventory_snapshots : warehouse_sk
    dim_date ||--o{ fact_inventory_snapshots : snapshot_date_key

    dim_customer ||--o{ fact_clickstream : customer_sk
    dim_product ||--o{ fact_clickstream : product_sk

    dim_customer {
        bigint customer_sk PK
        string customer_id NK
        string customer_segment "SCD2"
        string loyalty_status "SCD2"
        string country_code "SCD2"
        string email_masked "SCD1"
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }
    fact_orders {
        string order_id PK
        int order_date_key FK
        bigint customer_sk FK
        decimal subtotal_usd
        decimal discount_usd
        decimal total_amount_usd
        string order_status
        boolean is_paid
    }
```

## Fact tables and grain

| Fact | Grain (one row per…) | Natural key | Load pattern | Key measures |
|---|---|---|---|---|
| `fact_orders` | order (latest lifecycle state) | `order_id` | Incremental MERGE from `silver.orders` changes | subtotal, discount, shipping, tax, total (local + USD), item_count, `is_paid`, `is_cancelled`, fulfilment timestamps |
| `fact_order_items` | order line | `order_item_id` | Incremental MERGE | quantity, unit_price, line_discount, line_net (local + USD) |
| `fact_payments` | payment attempt | `payment_id` | Incremental MERGE | amount (local + USD), status, method, `is_success` |
| `fact_shipments` | shipment | `shipment_id` | Incremental MERGE | fulfilment hours, delivery hours, `is_on_time`, `is_delayed` |
| `fact_refunds` | refund transaction | `refund_id` | Incremental MERGE | refund_amount, refund_tax_amount (local + USD) |
| `fact_returns` | returned order line | `return_id` | Incremental MERGE | returned_quantity, return_reason |
| `fact_clickstream` | behavioural event | `event_id` | Incremental insert-if-absent | event flags (view / cart / checkout / purchase) |
| `fact_inventory_snapshots` | product × warehouse × day | (`snapshot_date`, `product_id`, `warehouse_id`) | Daily partition overwrite (`replaceWhere`) | available, reserved, on_hand, reorder_threshold, `is_stockout`, `is_low_stock` |

## Dimensions

| Dimension | SCD | Notes |
|---|---|---|
| `dim_customer` | **Type 2** on `customer_segment`, `loyalty_status`, `country_code`. **Type 1** on masked contact attributes. | Facts store the `customer_sk` valid at event time, so revenue keeps the segment the customer had at that moment. |
| `dim_product` | **Type 2** on `unit_price`, `product_status`, `category_id`, `supplier_id`. Type 1 on name/brand. | Price history supports pricing analysis. |
| `dim_category`, `dim_supplier`, `dim_warehouse`, `dim_promotion` | Type 1 | Overwrite in place. |
| `dim_country`, `dim_currency`, `dim_sales_channel` | Type 1 (static reference) | Seeded from reference data. |
| `dim_date` | Static, generated | 2020-01-01 … 2030-12-31; includes `is_weekend`, `is_black_friday`, fiscal attributes. |

### Surrogate keys, unknown and late-arriving members

* Surrogate keys are `BIGINT` values from `xxhash64(natural_key, valid_from)`.
  The hash is deterministic, so a rebuild or replay never re-numbers rows, and
  no sequence state has to be coordinated across jobs.
* Every dimension contains an **unknown member** (`sk = -1`, natural key
  `'UNKNOWN'`). Facts whose dimension lookup fails point to it. No fact is
  ever dropped for a missing dimension.
* **Late-arriving dimensions:** when a fact references a customer or product
  that has no dimension row yet (the Kafka event arrived before the Postgres
  extract), Gold inserts an **inferred member** (`is_inferred = true`) with the
  natural key and default attributes. The next dimension load updates the
  inferred row in place (Type 1). It does not open a new version, so facts stay
  attached.

## Marts

| Mart | Grain | Purpose |
|---|---|---|
| `mart_daily_sales` | date × country × channel | Revenue, GMV, orders, AOV, discount, refund and cancellation rates (order-level measures) |
| `mart_daily_category_sales` | date × product category | Units, net merchandise revenue, margin (line-level measures) |
| `mart_product_performance` | product × month | Units, revenue, views, conversion, return rate, rank |
| `mart_customer_360` | customer (current) | Unified profile, RFM, CLV, segment |
| `mart_customer_retention` | cohort month × months since first purchase | Cohort retention matrix |
| `mart_customer_lifetime_value` | customer | Historical CLV and its components |
| `mart_conversion_funnel` | date × device × traffic source | Sessions → product view → cart → checkout → purchase |
| `mart_inventory_health` | product × warehouse (latest snapshot) | Stock, stock-outs, low-stock, days of cover, reorder qty |
| `mart_order_fulfillment` | date × warehouse × carrier | Fulfilment and delivery times, on-time rate, SLA breaches |
| `mart_payment_performance` | date × payment method × country | Success / failure rates, amounts, reconciliation gaps |
| `mart_returns_analysis` | month × category × reason | Return rates and refund amounts |
| `mart_marketing_performance` | date × campaign × traffic source | Spend, sessions, orders, revenue, ROAS, CAC |
| `mart_executive_summary` | date | Headline KPIs with prior-period comparison |

### Preventing double counting

Orders, order items, payments and refunds have different grains
(1 order : N items : M payment attempts : K refunds). A naive join multiplies
order revenue by `N × M × K`. Every mart therefore:

1. **aggregates each fact to the order grain first** (`items_agg`, `payments_agg`, `refunds_agg` CTEs),
2. **joins the aggregates one-to-one on `order_id`**, and
3. only then rolls up to the mart grain.

`tests/unit/test_gold_marts.py::test_no_double_counting` builds an order with
3 items, 2 payment attempts and 2 partial refunds and asserts that revenue and
refund totals match the hand-calculated values.
