# KPI definitions

One definition per KPI, implemented identically in the Gold marts (`data_models/marts/*.sql`),
the Streamlit dashboard and the Power BI measures (`dashboards/powerbi/measures.dax`).

**Conventions**
* **Currency:** every KPI is reported in **USD**. Transaction amounts are converted once, in Silver, at the daily
  FX rate of the transaction date (forward-filled over weekends and holidays; `fx_rate_date` records the quote
  used). **Refunds are converted at the original order's rate**, so a fully refunded order nets to exactly 0.
* **Tax** (VAT / sales tax) is a pass-through liability and is excluded from all revenue KPIs.
* **Paid order:** an order with at least one `captured` payment. **Completed order:** paid and not cancelled.
* **Dates:** order KPIs use the order date (UTC). Refund amounts in `fact_orders` are attributed to the order
  date (cohort view); `fact_refunds` additionally supports refund-date (cash) views.
* **Late data:** the daily marts always include late-arriving events (MERGE-based). The real-time panel
  (`gold.rt_sales_minute`) is approximate beyond its 10-minute watermark.

## Sales

| KPI | Definition | Grain / source | Filters |
|---|---|---|---|
| GMV | Σ (subtotal + shipping) of **all placed orders**, before discounts, excluding tax | `fact_orders.gmv_usd` | any status |
| Gross revenue | Σ (subtotal − discount + shipping) of paid orders, excluding tax | `fact_orders.gross_revenue_usd` | `is_paid` |
| Net revenue | Gross revenue − refunds excluding the refunded tax | `fact_orders.net_revenue_usd` | `is_paid` |
| Total orders | Count of paid orders (placed orders reported separately) | `fact_orders` | `is_paid` |
| Average order value (AOV) | Gross revenue / paid orders | mart_daily_sales | `is_paid` |
| Revenue growth | (Net revenue current period / net revenue previous period of equal length) − 1; WoW = vs same weekday 7 days earlier | mart_executive_summary | – |
| Revenue by country / channel | Net revenue grouped by ship-to country / sales channel | mart_daily_sales | – |
| Revenue by category | Σ line net amount (qty × unit price − line discount) of paid order lines | mart_daily_category_sales | `is_paid` |
| Discount impact (rate) | Discounts / list-price subtotal of paid orders | mart_daily_sales | `is_paid` |
| Refund rate | Refunds excluding tax / gross revenue | mart_daily_sales | `is_paid` |
| Cancellation rate | Cancelled orders / placed orders (payment failures and customer cancellations) | mart_daily_sales | – |
| Gross margin | Σ (line net − unit cost × qty) of paid lines; margin % = margin / line net | mart_daily_category_sales | `is_paid` |

## Customers

| KPI | Definition |
|---|---|
| New customers | Customers whose **first paid order** falls in the period |
| Returning customers | Active customers − new customers |
| Active customers | Distinct customers with ≥ 1 paid order in the period |
| Customer retention rate (cohort) | Customers of acquisition cohort *M* (first completed order in month *M*) with ≥ 1 completed order in month *M+N* / cohort size (`mart_customer_retention`) |
| Repeat purchase rate | Customers with ≥ 2 completed orders / customers with ≥ 1 completed order |
| Historical customer lifetime value | Net revenue of the customer's completed orders to date (`historical_clv_usd`). Margin CLV = gross margin − refunds (ex tax) |
| RFM | NTILE(5) over customers with ≥ 1 completed order: **R** recency (most recent purchase = 5), **F** completed orders, **M** net spend; `rfm_code` = "RFM" digits |

### Customer segments

Evaluated in order; the first rule that matches wins (`mart_customer_360.customer_segment`):

| Segment | Rule |
|---|---|
| Prospect | No completed order |
| New | First completed order within the last 30 days |
| High-value | M score = 5 (top quintile of net spend) **and** purchased within 90 days |
| Loyal | ≥ 4 completed orders **and** purchased within 60 days |
| Frequent | F score ≥ 4 **and** purchased within 90 days |
| At-risk | ≥ 2 completed orders **and** last purchase 91–180 days ago |
| Inactive | Last purchase more than 180 days ago |
| Occasional | Everyone else |

Segments are recomputed on every Customer 360 refresh (incremental facts → full mart rebuild in seconds).
Segment history is available through `dim_customer` (SCD2 on account segment, loyalty, country).

## Conversion (session based)

Unit = reconstructed session (client session id split after 30 min inactivity; identity stitched at login).

| KPI | Definition |
|---|---|
| Product views | Count of `product_view` events |
| Add-to-cart rate | Sessions with ≥ 1 `add_to_cart` / sessions |
| Checkout initiation rate | Sessions with `checkout_started` / sessions |
| Purchase conversion rate | Sessions with `purchase` / sessions |
| Cart abandonment rate | Sessions with a cart but no purchase / sessions with a cart |
| Funnel by device / source | Same metrics grouped by `device_type` / `traffic_source` (`mart_conversion_funnel`) |

## Inventory

| KPI | Definition |
|---|---|
| Available stock | Opening snapshot + Σ available-quantity deltas of all movements since (order-independent) |
| Reserved stock | Units allocated to unshipped orders (same delta method) |
| Stock-out rate | Product × warehouse positions with available ≤ 0 / all positions |
| Low-stock products | Positions with 0 < available < reorder threshold |
| Inventory turnover (annualised) | Paid units in the last 30 days × 365/30 / on-hand units |
| Warehouse utilisation | On-hand units / warehouse capacity |
| Replenishment requirement | For positions at/below the threshold: 2 × threshold − available |

## Fulfilment

| KPI | Definition |
|---|---|
| Average fulfilment time | Hours from order confirmation to carrier hand-over (`shipped_at`) |
| Average delivery time | Days from carrier hand-over to proof of delivery (transit) and from order to delivery |
| On-time delivery rate | Delivered shipments with actual ≤ promised (estimated) delivery / delivered shipments |
| Delayed shipments | Shipments with a delay event, delivered late, or undelivered past the promised date |
| SLA violations | Shipments handed over more than 48 h after confirmation |
| Carrier performance | On-time rate, transit days and delays per carrier |
| Order cancellation rate | See sales |

## Payments

| KPI | Definition |
|---|---|
| Payment success rate | Captured attempts / (captured + failed) attempts; authorisations still pending are excluded |
| Payment failure rate | 1 − success rate |
| Refund amounts | Σ refund amount (incl. tax) by refund date |
| Payment method distribution | Attempts per method |
| Order-to-payment reconciliation difference | Σ (captured amount − order total) for paid orders; plus count of orders whose single capture deviates by > 0.01 (`mart_payment_performance`, `gold.financial_reconciliation`) |
