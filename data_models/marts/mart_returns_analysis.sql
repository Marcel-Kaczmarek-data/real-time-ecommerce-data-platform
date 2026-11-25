-- mart_returns_analysis
-- Grain: month x product category x return reason.
-- Return rate = returned units / units sold in the same category and month (by order month).
WITH rets AS (
    SELECT date_trunc('month', r.return_date) AS month, p.category_name, p.department, r.return_reason,
           COUNT(*) AS returns, SUM(r.returned_quantity) AS units_returned,
           SUM(r.returned_value_usd) AS returned_value_usd,
           SUM(CASE WHEN r.is_quality_issue THEN 1 ELSE 0 END) AS quality_returns
    FROM fact_returns r
    LEFT JOIN dim_product p ON p.product_sk = r.product_sk
    GROUP BY 1, 2, 3, 4
),

refunds AS (
    SELECT date_trunc('month', refund_date) AS month, SUM(refund_amount_usd) AS refunds_usd
    FROM fact_refunds WHERE refund_reason = 'product_return' GROUP BY 1
),

sold AS (
    SELECT date_trunc('month', i.order_date) AS month, c.category_name, SUM(i.quantity) AS units_sold
    FROM fact_order_items i INNER JOIN dim_category c ON c.category_sk = i.category_sk
    WHERE i.is_paid GROUP BY 1, 2
)

SELECT
    CAST(r.month AS DATE)                                  AS month,
    r.category_name, r.department, r.return_reason,
    r.returns, r.units_returned,
    ROUND(r.returned_value_usd, 2)                         AS returned_value_usd,
    r.quality_returns,
    COALESCE(s.units_sold, 0)                              AS category_units_sold,
    ROUND(r.units_returned / NULLIF(s.units_sold, 0), 4)   AS return_rate,
    ROUND(f.refunds_usd, 2)                                AS month_return_refunds_usd
FROM rets r
LEFT JOIN sold s ON s.month = r.month AND s.category_name <=> r.category_name
LEFT JOIN refunds f ON f.month = r.month
