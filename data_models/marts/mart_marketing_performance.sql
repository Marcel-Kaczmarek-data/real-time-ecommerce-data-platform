-- mart_marketing_performance
-- Grain: date x traffic source (paid and organic).
-- Attribution: last-touch session. An order is attributed to the traffic source
-- of the session that produced it (orders without a session, e.g. marketplace
-- or legacy history, are excluded from attribution).
-- ROAS = attributed gross revenue / spend. CAC = spend / new customers acquired
-- (customers whose first completed order is attributed to the source).
WITH spend AS (
    SELECT spend_date AS date, traffic_source, SUM(spend_usd) AS spend_usd,
           SUM(impressions) AS impressions, SUM(clicks) AS clicks
    FROM silver_marketing_spend GROUP BY spend_date, traffic_source
),

sess AS (
    SELECT session_date AS date, traffic_source, COUNT(*) AS sessions,
           SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS converted_sessions
    FROM silver_sessions GROUP BY session_date, traffic_source
),

attributed AS (
    SELECT o.order_date AS date, s.traffic_source, o.order_id, o.customer_id, o.gross_revenue_usd, o.is_paid,
           o.order_timestamp
    FROM fact_orders o
    INNER JOIN silver_orders so ON so.order_id = o.order_id
    INNER JOIN silver_sessions s ON s.session_id = so.session_id
),

first_orders AS (
    SELECT customer_id, MIN(order_timestamp) AS first_ts FROM fact_orders WHERE is_paid GROUP BY customer_id
),

orders_agg AS (
    SELECT a.date, a.traffic_source,
           SUM(CASE WHEN a.is_paid THEN 1 ELSE 0 END) AS attributed_orders,
           SUM(a.gross_revenue_usd) AS attributed_revenue_usd,
           COUNT(DISTINCT CASE WHEN a.is_paid AND a.order_timestamp = f.first_ts THEN a.customer_id END) AS new_customers
    FROM attributed a LEFT JOIN first_orders f ON f.customer_id = a.customer_id
    GROUP BY a.date, a.traffic_source
),

keys AS (
    SELECT date, traffic_source FROM sess
    UNION
    SELECT date, traffic_source FROM spend
)

SELECT
    k.date,
    CAST(date_format(k.date, 'yyyyMMdd') AS INT)                        AS date_key,
    k.traffic_source,
    k.traffic_source IN ('paid_search', 'social', 'email', 'affiliate') AS is_paid_channel,
    ROUND(COALESCE(sp.spend_usd, 0), 2)                                 AS spend_usd,
    COALESCE(sp.impressions, 0)                                         AS impressions,
    COALESCE(sp.clicks, 0)                                              AS clicks,
    COALESCE(se.sessions, 0)                                            AS sessions,
    COALESCE(o.attributed_orders, 0)                                    AS attributed_orders,
    ROUND(COALESCE(o.attributed_revenue_usd, 0), 2)                     AS attributed_revenue_usd,
    COALESCE(o.new_customers, 0)                                        AS new_customers,
    ROUND(COALESCE(o.attributed_orders, 0) / NULLIF(se.sessions, 0), 4)  AS session_conversion_rate,
    ROUND(o.attributed_revenue_usd / NULLIF(sp.spend_usd, 0), 2)         AS roas,
    ROUND(sp.spend_usd / NULLIF(o.new_customers, 0), 2)                  AS cac_usd,
    ROUND(sp.spend_usd / NULLIF(sp.clicks, 0), 2)                        AS cpc_usd
FROM keys k
LEFT JOIN spend sp ON sp.date = k.date AND sp.traffic_source = k.traffic_source
LEFT JOIN sess se ON se.date = k.date AND se.traffic_source = k.traffic_source
LEFT JOIN orders_agg o ON o.date = k.date AND o.traffic_source = k.traffic_source
