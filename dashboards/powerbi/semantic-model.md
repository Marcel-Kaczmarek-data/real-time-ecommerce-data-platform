# Power BI semantic model specification

## Tables

| Table | Type | Key | Notes |
|---|---|---|---|
| `dim_date` | Dimension | `date_key` (int yyyymmdd) | **Mark as date table** on `date`. Sort `month_name` by `month`, `day_name` by `day_of_week`. |
| `dim_customer` | Dimension (SCD2) | `customer_sk` | All versions. For "current" slicers use `is_current = TRUE`. Hide `valid_from`, `valid_to`, `version`. |
| `dim_product` | Dimension (SCD2) | `product_sk` | `category_name` / `department` denormalised for convenience. |
| `dim_category` | Dimension | `category_sk` | |
| `dim_country` | Dimension | `country_sk` | Data category: `country_code` = Country/Region, `country_name` = Country. |
| `dim_sales_channel` | Dimension | `channel_sk` | |
| `dim_warehouse` | Dimension | `warehouse_sk` | |
| `dim_promotion` | Dimension | `promotion_sk` | `-2` = no promotion, `-1` = unknown. |
| `fact_orders` | Fact (order grain) | `order_id` | Main revenue fact. |
| `fact_order_items` | Fact (line grain) | `order_item_id` | Product / category revenue and margin. |
| `fact_payments` | Fact (payment attempt) | `payment_id` | |
| `fact_shipments` | Fact (shipment) | `shipment_id` | |
| `fact_refunds` | Fact (refund) | `refund_id` | |
| `fact_returns` | Fact (returned line) | `return_id` | |
| `mart_customer_360` | Snapshot table | `customer_id` | Segments, RFM, CLV; relate to `dim_customer[customer_id]` only for current-profile analysis (many-to-one on current rows). |
| `mart_customer_retention` | Aggregate | (cohort_month, months_since_first) | Matrix visual. No relationships. |
| `mart_conversion_funnel` | Aggregate | (date, device, source, channel) | Relate `date_key` → `dim_date`. |
| `mart_inventory_health` | Snapshot | (product_id, warehouse_id) | Stand-alone page. |

## Relationships (all single direction, many-to-one, dimension filters fact)

| From (many) | To (one) | Active |
|---|---|---|
| `fact_orders[order_date_key]` | `dim_date[date_key]` | yes |
| `fact_orders[customer_sk]` | `dim_customer[customer_sk]` | yes |
| `fact_orders[country_sk]` | `dim_country[country_sk]` | yes |
| `fact_orders[channel_sk]` | `dim_sales_channel[channel_sk]` | yes |
| `fact_orders[promotion_sk]` | `dim_promotion[promotion_sk]` | yes |
| `fact_order_items[order_date_key]` | `dim_date[date_key]` | yes |
| `fact_order_items[product_sk]` | `dim_product[product_sk]` | yes |
| `fact_order_items[category_sk]` | `dim_category[category_sk]` | yes |
| `fact_order_items[customer_sk]` | `dim_customer[customer_sk]` | yes |
| `fact_payments[payment_date_key]` | `dim_date[date_key]` | yes |
| `fact_payments[country_sk]` | `dim_country[country_sk]` | yes |
| `fact_shipments[ship_date_key]` | `dim_date[date_key]` | yes |
| `fact_shipments[warehouse_sk]` | `dim_warehouse[warehouse_sk]` | yes |
| `fact_refunds[refund_date_key]` | `dim_date[date_key]` | yes |
| `fact_returns[return_date_key]` | `dim_date[date_key]` | yes |
| `fact_returns[product_sk]` | `dim_product[product_sk]` | yes |
| `mart_conversion_funnel[date_key]` | `dim_date[date_key]` | yes |

There are no fact-to-fact relationships, so double counting cannot occur.
Payment and refund amounts per order are already pre-aggregated in
`fact_orders` (`captured_amount_usd`, `refund_amount_usd`).

## Model hygiene

* Hide all surrogate keys and foreign keys from report view.
* Set the format strings `"$"#,0` for `*_usd` columns and `0.0%` for rates.
* Summarization: `None` for ids, keys and rates; `Sum` only for additive amounts.
* Row-level security (optional): role `CountryManager` with
  `dim_country[country_code] = USERPRINCIPALNAME()`-mapped country via a security table.
