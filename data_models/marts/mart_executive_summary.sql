-- mart_executive_summary
-- Grain: calendar day. Headline KPIs plus 7-day rolling values and the
-- comparison with the same day one week earlier (WoW growth).
WITH sales AS (
    SELECT date, SUM(placed_orders) AS placed_orders, SUM(paid_orders) AS paid_orders,
           SUM(cancelled_orders) AS cancelled_orders, SUM(gmv_usd) AS gmv_usd,
           SUM(gross_revenue_usd) AS gross_revenue_usd, SUM(net_revenue_usd) AS net_revenue_usd,
           SUM(discount_usd) AS discount_usd, SUM(refunds_net_of_tax_usd) AS refunds_usd,
           SUM(units_sold) AS units_sold
    FROM mart_daily_sales GROUP BY date
),

customers AS (
    SELECT order_date AS date,
           COUNT(DISTINCT customer_id) AS active_customers,
           COUNT(DISTINCT CASE WHEN is_first THEN customer_id END) AS new_customers
    FROM (SELECT order_date, customer_id,
                 order_timestamp = MIN(order_timestamp) OVER (PARTITION BY customer_id) AS is_first
          FROM fact_orders WHERE is_paid) GROUP BY order_date
),

funnel AS (
    SELECT date, SUM(sessions) AS sessions, SUM(converted_sessions) AS converted_sessions,
           SUM(sessions_with_cart) AS carts, SUM(sessions_with_cart) - SUM(converted_sessions) AS abandoned_carts
    FROM mart_conversion_funnel GROUP BY date
),

pay AS (
    SELECT date, SUM(successful_payments) AS ok, SUM(failed_payments) AS ko FROM mart_payment_performance GROUP BY date
),

ship AS (
    SELECT date, SUM(delivered_shipments) AS delivered,
           SUM(on_time_delivery_rate * delivered_shipments) AS on_time FROM mart_order_fulfillment GROUP BY date
),

daily AS (
    SELECT d.date, d.date_key, d.is_weekend, d.is_black_friday,
           COALESCE(s.placed_orders, 0) AS placed_orders, COALESCE(s.paid_orders, 0) AS paid_orders,
           COALESCE(s.gmv_usd, 0) AS gmv_usd, COALESCE(s.gross_revenue_usd, 0) AS gross_revenue_usd,
           COALESCE(s.net_revenue_usd, 0) AS net_revenue_usd, COALESCE(s.discount_usd, 0) AS discount_usd,
           COALESCE(s.refunds_usd, 0) AS refunds_usd, COALESCE(s.units_sold, 0) AS units_sold,
           COALESCE(c.active_customers, 0) AS active_customers, COALESCE(c.new_customers, 0) AS new_customers,
           COALESCE(s.cancelled_orders, 0) AS cancelled_orders,
           f.sessions, f.converted_sessions, f.carts, f.abandoned_carts, p.ok, p.ko, sh.delivered, sh.on_time
    FROM dim_date d
    LEFT JOIN sales s ON s.date = d.date
    LEFT JOIN customers c ON c.date = d.date
    LEFT JOIN funnel f ON f.date = d.date
    LEFT JOIN pay p ON p.date = d.date
    LEFT JOIN ship sh ON sh.date = d.date
    WHERE d.date BETWEEN (SELECT MIN(fo.order_date) FROM fact_orders AS fo) AND current_date()
)

SELECT
    date, date_key, is_weekend, is_black_friday,
    placed_orders, paid_orders, cancelled_orders, units_sold,
    ROUND(gmv_usd, 2) AS gmv_usd, ROUND(gross_revenue_usd, 2) AS gross_revenue_usd,
    ROUND(net_revenue_usd, 2) AS net_revenue_usd, ROUND(discount_usd, 2) AS discount_usd,
    ROUND(refunds_usd, 2) AS refunds_usd,
    ROUND(gross_revenue_usd / NULLIF(paid_orders, 0), 2)              AS aov_usd,
    active_customers, new_customers, active_customers - new_customers AS returning_customers,
    sessions,
    ROUND(converted_sessions / NULLIF(sessions, 0), 4)                AS conversion_rate,
    ROUND(abandoned_carts / NULLIF(carts, 0), 4)                      AS cart_abandonment_rate,
    ROUND(ok / NULLIF(ok + ko, 0), 4)                                 AS payment_success_rate,
    ROUND(on_time / NULLIF(delivered, 0), 4)                          AS on_time_delivery_rate,
    ROUND(refunds_usd / NULLIF(gross_revenue_usd, 0), 4)              AS refund_rate,
    ROUND(cancelled_orders / NULLIF(placed_orders, 0), 4)             AS cancellation_rate,
    ROUND(SUM(net_revenue_usd) OVER w7, 2)                            AS net_revenue_7d_usd,
    SUM(paid_orders) OVER w7                                          AS paid_orders_7d,
    ROUND(net_revenue_usd / NULLIF(LAG(net_revenue_usd, 7) OVER (ORDER BY date), 0) - 1, 4) AS net_revenue_wow_growth,
    ROUND(paid_orders / NULLIF(LAG(paid_orders, 7) OVER (ORDER BY date), 0) - 1, 4)         AS orders_wow_growth,
    ROUND(SUM(net_revenue_usd) OVER w7 / NULLIF(SUM(net_revenue_usd) OVER w7prev, 0) - 1, 4) AS net_revenue_7d_growth
FROM daily
WINDOW w7 AS (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
       w7prev AS (ORDER BY date ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING)
