-- mart_payment_performance
-- Grain: payment date x payment method x country.
-- Success rate = captured attempts / (captured + failed) attempts.
-- Reconciliation: captured amount vs total of the paid orders, both at
-- order grain. A non-zero difference flags orders whose captured amount does not
-- match the order total (partial capture, duplicates, currency errors).
WITH attempts AS (
    SELECT payment_date, payment_method, country_code,
           COUNT(*)                                                AS payment_attempts,
           SUM(CASE WHEN is_success THEN 1 ELSE 0 END)             AS successful_payments,
           SUM(CASE WHEN is_failure THEN 1 ELSE 0 END)             AS failed_payments,
           SUM(CASE WHEN is_success THEN payment_amount_usd ELSE 0 END) AS captured_amount_usd,
           SUM(CASE WHEN is_failure THEN payment_amount_usd ELSE 0 END) AS failed_amount_usd
    FROM fact_payments GROUP BY payment_date, payment_method, country_code
),

order_level AS (
    SELECT o.order_id, o.country_code, CAST(o.paid_at AS DATE) AS paid_date, o.total_amount_usd,
           o.captured_amount_usd, p.payment_method
    FROM fact_orders o
    INNER JOIN (SELECT fp.order_id, MAX_BY(fp.payment_method, fp.captured_at) AS payment_method
          FROM fact_payments AS fp WHERE fp.is_success GROUP BY fp.order_id) p ON p.order_id = o.order_id
    WHERE o.is_paid
),

recon AS (
    SELECT paid_date, payment_method, country_code,
           SUM(total_amount_usd) AS paid_order_total_usd,
           SUM(captured_amount_usd - total_amount_usd) AS reconciliation_difference_usd,
           SUM(CASE WHEN ABS(captured_amount_usd - total_amount_usd) > 0.01 THEN 1 ELSE 0 END) AS unreconciled_orders
    FROM order_level GROUP BY paid_date, payment_method, country_code
),

refunds AS (
    SELECT r.refund_date, p.payment_method, r.country_code,
           SUM(r.refund_amount_usd) AS refunded_amount_usd, COUNT(*) AS refunds
    FROM fact_refunds r INNER JOIN fact_payments p ON p.payment_id = r.payment_id
    GROUP BY r.refund_date, p.payment_method, r.country_code
)

SELECT
    a.payment_date                                   AS date,
    CAST(date_format(a.payment_date, 'yyyyMMdd') AS INT) AS date_key,
    a.payment_method, a.country_code,
    a.payment_attempts, a.successful_payments, a.failed_payments,
    ROUND(a.successful_payments / NULLIF(a.successful_payments + a.failed_payments, 0), 4) AS payment_success_rate,
    ROUND(a.failed_payments / NULLIF(a.successful_payments + a.failed_payments, 0), 4)     AS payment_failure_rate,
    ROUND(a.captured_amount_usd, 2)                  AS captured_amount_usd,
    ROUND(a.failed_amount_usd, 2)                    AS failed_amount_usd,
    ROUND(COALESCE(r.refunded_amount_usd, 0), 2)     AS refunded_amount_usd,
    COALESCE(r.refunds, 0)                           AS refunds,
    ROUND(COALESCE(rc.paid_order_total_usd, 0), 2)   AS paid_order_total_usd,
    ROUND(COALESCE(rc.reconciliation_difference_usd, 0), 2) AS reconciliation_difference_usd,
    COALESCE(rc.unreconciled_orders, 0)              AS unreconciled_orders
FROM attempts a
LEFT JOIN recon rc ON rc.paid_date = a.payment_date AND rc.payment_method = a.payment_method
                  AND rc.country_code <=> a.country_code
LEFT JOIN refunds r ON r.refund_date = a.payment_date AND r.payment_method = a.payment_method
                   AND r.country_code <=> a.country_code
