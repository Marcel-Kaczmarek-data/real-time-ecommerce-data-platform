-- mart_order_fulfillment
-- Grain: ship date x warehouse x carrier (shipment grain aggregated).
-- On-time = delivered on or before the promised (estimated) delivery date.
-- SLA breach = more than 48 hours from order confirmation to carrier hand-over.
SELECT
    s.ship_date                                                    AS date,
    s.ship_date_key                                                AS date_key,
    s.warehouse_id, w.warehouse_name, w.region,
    s.carrier,
    COUNT(*)                                                       AS shipments,
    SUM(CASE WHEN s.is_delivered THEN 1 ELSE 0 END)                AS delivered_shipments,
    SUM(CASE WHEN s.is_delayed THEN 1 ELSE 0 END)                  AS delayed_shipments,
    SUM(CASE WHEN s.is_sla_breach THEN 1 ELSE 0 END)               AS sla_breaches,
    ROUND(AVG(s.fulfillment_hours), 2)                             AS avg_fulfillment_hours,
    ROUND(percentile_approx(s.fulfillment_hours, 0.9), 2)          AS p90_fulfillment_hours,
    ROUND(AVG(s.transit_days), 2)                                  AS avg_transit_days,
    ROUND(AVG(s.order_to_delivery_days), 2)                        AS avg_order_to_delivery_days,
    ROUND(SUM(CASE WHEN s.is_on_time THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN s.is_delivered THEN 1 ELSE 0 END), 0), 4) AS on_time_delivery_rate,
    ROUND(AVG(CASE WHEN s.is_delayed THEN s.days_late END), 2)     AS avg_days_late_when_delayed
FROM fact_shipments s
LEFT JOIN dim_warehouse w ON w.warehouse_sk = s.warehouse_sk
WHERE s.ship_date IS NOT NULL
GROUP BY s.ship_date, s.ship_date_key, s.warehouse_id, w.warehouse_name, w.region, s.carrier
