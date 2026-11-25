-- mart_daily_category_sales
-- Grain: order date x product category. Item-level measures (category is an
-- attribute of the order line, not of the order). Paid orders only.
SELECT
    i.order_date                                  AS date,
    i.order_date_key                              AS date_key,
    cat.category_id,
    cat.category_name,
    cat.department,
    COUNT(DISTINCT i.order_id)                    AS orders,
    SUM(i.quantity)                               AS units_sold,
    ROUND(SUM(i.line_gross_amount_usd), 2)        AS gross_merchandise_usd,
    ROUND(SUM(i.discount_amount_usd), 2)          AS discount_usd,
    ROUND(SUM(i.line_net_amount_usd), 2)          AS net_merchandise_usd,
    ROUND(SUM(i.line_cost_usd), 2)                AS cost_usd,
    ROUND(SUM(i.line_margin_usd), 2)              AS gross_margin_usd,
    ROUND(SUM(i.line_margin_usd) / NULLIF(SUM(i.line_net_amount_usd), 0), 4) AS gross_margin_pct
FROM fact_order_items i
INNER JOIN dim_category cat ON cat.category_sk = i.category_sk
WHERE i.is_paid
GROUP BY i.order_date, i.order_date_key, cat.category_id, cat.category_name, cat.department
