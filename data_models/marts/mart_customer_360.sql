-- mart_customer_360
-- Grain: one row per customer (current dimension version).
-- Unified profile: master data + orders + payments/refunds + returns + sessions.
-- "Completed order" = paid and not cancelled. Monetary values are net of refunds,
-- excluding tax, in USD. As-of date = current_date().
-- Segmentation rules (first matching rule wins), see docs/kpi-definitions.md#customer-segments:
--   Prospect     no completed order
--   New          first completed order within the last 30 days
--   High-value   monetary score 5 (top quintile) and purchased within 90 days
--   Loyal        >= 4 completed orders and purchased within 60 days
--   Frequent     frequency score >= 4 and purchased within 90 days
--   At-risk      >= 2 completed orders and last purchase 91-180 days ago
--   Inactive     last purchase more than 180 days ago
--   Occasional   everyone else
WITH completed AS (
    SELECT * FROM fact_orders WHERE is_paid AND NOT is_cancelled
),

orders_agg AS (
    SELECT customer_id,
           MIN(order_date)                                   AS first_purchase_date,
           MAX(order_date)                                   AS last_purchase_date,
           COUNT(*)                                          AS completed_orders,
           SUM(net_revenue_usd)                              AS total_net_spend_usd,
           SUM(gross_revenue_usd)                            AS total_gross_spend_usd,
           SUM(refund_amount_usd)                            AS total_refunds_usd,
           SUM(CASE WHEN is_fully_returned THEN 1 ELSE 0 END) AS fully_returned_orders
    FROM completed GROUP BY customer_id
),

channel_pref AS (
    SELECT customer_id, sales_channel AS preferred_channel FROM (
        SELECT customer_id, sales_channel,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC, sales_channel) AS rn
        FROM completed GROUP BY customer_id, sales_channel) WHERE rn = 1
),

category_pref AS (
    SELECT customer_id, category_name AS favorite_category FROM (
        SELECT i.customer_id, c.category_name,
               ROW_NUMBER() OVER (PARTITION BY i.customer_id
                                  ORDER BY SUM(i.line_net_amount_usd) DESC, c.category_name) AS rn
        FROM fact_order_items i INNER JOIN dim_category c ON c.category_sk = i.category_sk
        WHERE i.is_paid GROUP BY i.customer_id, c.category_name) WHERE rn = 1
),

returns_agg AS (
    SELECT o.customer_id, COUNT(*) AS total_returns, SUM(r.returned_quantity) AS units_returned
    FROM fact_returns r INNER JOIN fact_orders o ON o.order_id = r.order_id GROUP BY o.customer_id
),

sessions_agg AS (
    SELECT customer_id, MAX(session_start) AS most_recent_session, COUNT(*) AS sessions_90d_or_more
    FROM silver_sessions WHERE customer_id IS NOT NULL GROUP BY customer_id
),

base AS (
    SELECT d.customer_id, d.customer_sk, d.first_name, d.last_name_initial, d.email_masked, d.country_code,
           d.customer_segment AS account_type, d.loyalty_status, d.registration_date, d.marketing_opt_in,
           o.first_purchase_date, o.last_purchase_date,
           COALESCE(o.completed_orders, 0)                        AS total_completed_orders,
           ROUND(COALESCE(o.total_net_spend_usd, 0), 2)           AS total_net_spend_usd,
           ROUND(COALESCE(o.total_refunds_usd, 0), 2)             AS total_refunds_usd,
           ROUND(o.total_gross_spend_usd / NULLIF(o.completed_orders, 0), 2) AS average_order_value_usd,
           cp.favorite_category, ch.preferred_channel,
           s.most_recent_session,
           COALESCE(ra.total_returns, 0)                          AS total_returns,
           DATEDIFF(current_date(), o.last_purchase_date)         AS days_since_last_purchase,
           DATEDIFF(current_date(), o.first_purchase_date)        AS days_since_first_purchase
    FROM dim_customer d
    LEFT JOIN orders_agg o     ON o.customer_id = d.customer_id
    LEFT JOIN category_pref cp ON cp.customer_id = d.customer_id
    LEFT JOIN channel_pref ch  ON ch.customer_id = d.customer_id
    LEFT JOIN returns_agg ra   ON ra.customer_id = d.customer_id
    LEFT JOIN sessions_agg s   ON s.customer_id = d.customer_id
    WHERE d.is_current AND d.customer_id <> 'UNKNOWN'
),

scored AS (
    SELECT *,
           -- Recency: oldest purchase -> 1, most recent -> 5.
           CASE WHEN total_completed_orders > 0 THEN NTILE(5) OVER (
                PARTITION BY (total_completed_orders > 0) ORDER BY days_since_last_purchase DESC) END AS r_score,
           CASE WHEN total_completed_orders > 0 THEN NTILE(5) OVER (
                PARTITION BY (total_completed_orders > 0) ORDER BY total_completed_orders) END AS f_score,
           CASE WHEN total_completed_orders > 0 THEN NTILE(5) OVER (
                PARTITION BY (total_completed_orders > 0) ORDER BY total_net_spend_usd) END AS m_score
    FROM base
)

SELECT
    customer_id, customer_sk, first_name, last_name_initial, email_masked, country_code, account_type,
    loyalty_status, registration_date, marketing_opt_in, first_purchase_date, last_purchase_date,
    total_completed_orders, total_net_spend_usd, average_order_value_usd, favorite_category, preferred_channel,
    most_recent_session, total_returns, total_refunds_usd,
    total_net_spend_usd                                   AS historical_clv_usd,
    days_since_last_purchase,
    r_score, f_score, m_score,
    CASE WHEN r_score IS NULL THEN NULL ELSE CONCAT(r_score, f_score, m_score) END AS rfm_code,
    CASE
        WHEN total_completed_orders = 0                                            THEN 'Prospect'
        WHEN days_since_first_purchase <= 30                                       THEN 'New'
        WHEN m_score = 5 AND days_since_last_purchase <= 90                        THEN 'High-value'
        WHEN total_completed_orders >= 4 AND days_since_last_purchase <= 60        THEN 'Loyal'
        WHEN f_score >= 4 AND days_since_last_purchase <= 90                       THEN 'Frequent'
        WHEN total_completed_orders >= 2 AND days_since_last_purchase BETWEEN 91 AND 180 THEN 'At-risk'
        WHEN days_since_last_purchase > 180                                        THEN 'Inactive'
        ELSE 'Occasional'
    END                                                    AS customer_segment,
    current_date()                                         AS as_of_date
FROM scored
