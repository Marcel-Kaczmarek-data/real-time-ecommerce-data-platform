-- mart_conversion_funnel
-- Grain: session date x device x traffic source x sales channel.
-- Unit of analysis is the reconstructed session (silver.customer_sessions):
--   sessions -> product view -> add to cart -> checkout -> purchase.
-- Rates are step conversion from all sessions; cart abandonment = carts without purchase / carts.
SELECT
    session_date                                                   AS date,
    CAST(date_format(session_date, 'yyyyMMdd') AS INT)             AS date_key,
    device_type,
    traffic_source,
    sales_channel,
    COUNT(*)                                                       AS sessions,
    SUM(CASE WHEN product_views > 0 THEN 1 ELSE 0 END)             AS sessions_with_product_view,
    SUM(CASE WHEN has_cart THEN 1 ELSE 0 END)                      AS sessions_with_cart,
    SUM(CASE WHEN reached_checkout THEN 1 ELSE 0 END)              AS sessions_with_checkout,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END)                     AS converted_sessions,
    SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END)                     AS bounced_sessions,
    SUM(product_views)                                             AS product_views,
    ROUND(SUM(CASE WHEN has_cart THEN 1 ELSE 0 END) / COUNT(*), 4)              AS add_to_cart_rate,
    ROUND(SUM(CASE WHEN reached_checkout THEN 1 ELSE 0 END) / COUNT(*), 4)      AS checkout_rate,
    ROUND(SUM(CASE WHEN converted THEN 1 ELSE 0 END) / COUNT(*), 4)             AS conversion_rate,
    ROUND(SUM(CASE WHEN is_cart_abandoned THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN has_cart THEN 1 ELSE 0 END), 0), 4)            AS cart_abandonment_rate,
    ROUND(SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) / COUNT(*), 4)             AS bounce_rate,
    ROUND(AVG(session_duration_seconds), 1)                                     AS avg_session_seconds
FROM silver_sessions
GROUP BY session_date, device_type, traffic_source, sales_channel
