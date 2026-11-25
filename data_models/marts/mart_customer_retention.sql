-- mart_customer_retention
-- Grain: acquisition cohort month x months since first purchase.
-- Cohort = month of a customer's first completed order. A customer is "retained"
-- in month N if they placed >= 1 completed order in that month.
WITH completed AS (
    SELECT customer_id, date_trunc('month', order_date) AS order_month
    FROM fact_orders WHERE is_paid AND NOT is_cancelled
),

first_month AS (
    SELECT customer_id, MIN(order_month) AS cohort_month FROM completed GROUP BY customer_id
),

activity AS (
    SELECT DISTINCT c.customer_id, f.cohort_month,
           CAST(months_between(c.order_month, f.cohort_month) AS INT) AS months_since_first
    FROM completed c INNER JOIN first_month f ON f.customer_id = c.customer_id
),

cohort_size AS (
    SELECT cohort_month, COUNT(*) AS cohort_customers FROM first_month GROUP BY cohort_month
)

SELECT
    CAST(a.cohort_month AS DATE)                      AS cohort_month,
    a.months_since_first,
    s.cohort_customers,
    COUNT(DISTINCT a.customer_id)                     AS active_customers,
    ROUND(COUNT(DISTINCT a.customer_id) / s.cohort_customers, 4) AS retention_rate
FROM activity a INNER JOIN cohort_size s ON s.cohort_month = a.cohort_month
GROUP BY a.cohort_month, a.months_since_first, s.cohort_customers
