-- mart_customer_lifetime_value
-- Grain: customer with >= 1 completed order.
-- Historical CLV = net revenue to date (USD, ex tax, after refunds).
-- Margin CLV     = gross margin of paid lines - refunds ex tax.
-- Purchase frequency = completed orders per 30 days of tenure (min tenure 30 days).
WITH completed AS (
    SELECT * FROM fact_orders WHERE is_paid AND NOT is_cancelled
),

margin AS (
    SELECT customer_id, SUM(line_margin_usd) AS gross_margin_usd
    FROM fact_order_items WHERE is_paid AND order_status <> 'cancelled' GROUP BY customer_id
),

agg AS (
    SELECT customer_id,
           MIN(order_date) AS first_order_date, MAX(order_date) AS last_order_date,
           COUNT(*) AS completed_orders,
           SUM(gross_revenue_usd) AS gross_revenue_usd,
           SUM(net_revenue_usd) AS net_revenue_usd,
           SUM(refund_amount_usd - refund_tax_amount_usd) AS refunds_net_usd
    FROM completed GROUP BY customer_id
)

SELECT
    a.customer_id,
    c.country_code, c.customer_segment AS account_type, c.loyalty_status,
    a.first_order_date, a.last_order_date,
    GREATEST(DATEDIFF(current_date(), a.first_order_date), 30)              AS tenure_days,
    a.completed_orders,
    ROUND(a.gross_revenue_usd / a.completed_orders, 2)                       AS average_order_value_usd,
    ROUND(a.completed_orders / (GREATEST(DATEDIFF(current_date(), a.first_order_date), 30) / 30.0), 3)
                                                                             AS orders_per_30_days,
    ROUND(a.net_revenue_usd, 2)                                              AS historical_clv_usd,
    ROUND(COALESCE(m.gross_margin_usd, 0) - a.refunds_net_usd, 2)            AS margin_clv_usd,
    CASE WHEN a.net_revenue_usd >= 1000 THEN 'Tier 1 (>= 1000)'
         WHEN a.net_revenue_usd >= 300  THEN 'Tier 2 (300-999)'
         WHEN a.net_revenue_usd >= 100  THEN 'Tier 3 (100-299)'
         ELSE 'Tier 4 (< 100)' END                                           AS clv_tier,
    PERCENT_RANK() OVER (ORDER BY a.net_revenue_usd)                         AS clv_percentile
FROM agg a
LEFT JOIN margin m ON m.customer_id = a.customer_id
LEFT JOIN dim_customer c ON c.customer_id = a.customer_id AND c.is_current
