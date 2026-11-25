-- mart_daily_sales
-- Grain: order date x ship-to country x sales channel.
-- Order-level measures only: fact_orders already holds exactly one row per
-- order, with payments and refunds pre-aggregated to order grain, so no
-- double counting is possible. KPI definitions: docs/kpi-definitions.md.
SELECT
    o.order_date                                                   AS date,
    o.order_date_key                                               AS date_key,
    o.country_code,
    c.country_name,
    c.region,
    o.sales_channel,
    COUNT(*)                                                       AS placed_orders,
    SUM(CASE WHEN o.is_paid THEN 1 ELSE 0 END)                     AS paid_orders,
    SUM(CASE WHEN o.is_cancelled THEN 1 ELSE 0 END)                AS cancelled_orders,
    SUM(o.gmv_usd)                                                 AS gmv_usd,
    SUM(o.gross_revenue_usd)                                       AS gross_revenue_usd,
    SUM(o.net_revenue_usd)                                         AS net_revenue_usd,
    SUM(CASE WHEN o.is_paid THEN o.discount_amount_usd ELSE 0 END) AS discount_usd,
    SUM(CASE WHEN o.is_paid THEN o.shipping_amount_usd ELSE 0 END) AS shipping_revenue_usd,
    SUM(CASE WHEN o.is_paid THEN o.tax_amount_usd ELSE 0 END)      AS tax_collected_usd,
    SUM(CASE WHEN o.is_paid THEN o.refund_amount_usd - o.refund_tax_amount_usd ELSE 0 END) AS refunds_net_of_tax_usd,
    SUM(CASE WHEN o.is_paid THEN o.item_count ELSE 0 END)          AS units_sold,
    COUNT(DISTINCT CASE WHEN o.is_paid THEN o.customer_id END)     AS purchasing_customers,
    ROUND(SUM(o.gross_revenue_usd) / NULLIF(SUM(CASE WHEN o.is_paid THEN 1 ELSE 0 END), 0), 2) AS aov_usd,
    ROUND(SUM(CASE WHEN o.is_paid THEN o.discount_amount_usd ELSE 0 END)
          / NULLIF(SUM(CASE WHEN o.is_paid THEN o.subtotal_amount_usd ELSE 0 END), 0), 4)       AS discount_rate,
    ROUND(SUM(CASE WHEN o.is_paid THEN o.refund_amount_usd - o.refund_tax_amount_usd ELSE 0 END)
          / NULLIF(SUM(o.gross_revenue_usd), 0), 4)                                             AS refund_rate,
    ROUND(SUM(CASE WHEN o.is_cancelled THEN 1 ELSE 0 END) / COUNT(*), 4)                        AS cancellation_rate
FROM fact_orders o
LEFT JOIN dim_country c ON c.country_sk = o.country_sk
GROUP BY o.order_date, o.order_date_key, o.country_code, c.country_name, c.region, o.sales_channel
