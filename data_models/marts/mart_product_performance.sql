-- mart_product_performance
-- Grain: product x calendar month. Sales (paid lines), behaviour (clickstream)
-- and returns are aggregated separately to product-month, then joined 1:1.
WITH sales AS (
    SELECT product_id, date_trunc('month', order_date) AS month,
           COUNT(DISTINCT order_id) AS orders, SUM(quantity) AS units_sold,
           SUM(line_net_amount_usd) AS revenue_usd, SUM(line_margin_usd) AS margin_usd
    FROM fact_order_items WHERE is_paid
    GROUP BY 1, 2
),

behaviour AS (
    SELECT product_id, date_trunc('month', event_date) AS month,
           SUM(CAST(is_product_view AS INT)) AS product_views,
           SUM(CAST(is_add_to_cart AS INT)) AS add_to_cart_events,
           COUNT(DISTINCT CASE WHEN is_product_view THEN session_id END) AS viewing_sessions
    FROM fact_clickstream WHERE product_id IS NOT NULL
    GROUP BY 1, 2
),

rets AS (
    SELECT product_id, date_trunc('month', return_date) AS month,
           SUM(returned_quantity) AS units_returned, SUM(returned_value_usd) AS returned_value_usd
    FROM fact_returns GROUP BY 1, 2
),

keys AS (
    SELECT product_id, month FROM sales
    UNION
    SELECT product_id, month FROM behaviour
    UNION
    SELECT product_id, month FROM rets
)

SELECT
    CAST(k.month AS DATE)                                           AS month,
    k.product_id,
    p.product_name, p.brand, p.category_name, p.department, p.unit_price AS current_list_price_usd,
    COALESCE(s.orders, 0)                                           AS orders,
    COALESCE(s.units_sold, 0)                                       AS units_sold,
    ROUND(COALESCE(s.revenue_usd, 0), 2)                            AS revenue_usd,
    ROUND(COALESCE(s.margin_usd, 0), 2)                             AS margin_usd,
    COALESCE(b.product_views, 0)                                    AS product_views,
    COALESCE(b.add_to_cart_events, 0)                               AS add_to_cart_events,
    ROUND(COALESCE(b.add_to_cart_events, 0) / NULLIF(b.product_views, 0), 4)  AS add_to_cart_rate,
    ROUND(COALESCE(s.orders, 0) / NULLIF(b.viewing_sessions, 0), 4)           AS view_to_order_rate,
    COALESCE(r.units_returned, 0)                                   AS units_returned,
    ROUND(COALESCE(r.units_returned, 0) / NULLIF(s.units_sold, 0), 4)         AS return_rate,
    DENSE_RANK() OVER (PARTITION BY k.month ORDER BY COALESCE(s.revenue_usd, 0) DESC) AS revenue_rank
FROM keys k
LEFT JOIN sales s ON s.product_id = k.product_id AND s.month = k.month
LEFT JOIN behaviour b ON b.product_id = k.product_id AND b.month = k.month
LEFT JOIN rets r ON r.product_id = k.product_id AND r.month = k.month
LEFT JOIN dim_product p ON p.product_id = k.product_id AND p.is_current
