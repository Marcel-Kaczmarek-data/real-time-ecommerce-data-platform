-- mart_inventory_health
-- Grain: product x warehouse, latest daily snapshot.
-- Demand = paid units over the last 30 days, split evenly across the
-- warehouses stocking the product. Days of cover = available / daily demand.
-- Replenishment requirement = units to reach 2x reorder threshold when at or below it.
WITH latest AS (
    SELECT f.* FROM fact_inventory_snapshots AS f
    WHERE f.snapshot_date = (SELECT MAX(s.snapshot_date) FROM fact_inventory_snapshots AS s)
),

demand AS (
    SELECT product_id, SUM(quantity) / 30.0 AS daily_units
    FROM fact_order_items
    WHERE is_paid AND order_date >= date_sub(current_date(), 30)
    GROUP BY product_id
),

sites AS (
    SELECT product_id, COUNT(*) AS stocking_sites FROM latest GROUP BY product_id
),

wh_load AS (
    SELECT warehouse_id, SUM(on_hand_quantity) AS on_hand_units FROM latest GROUP BY warehouse_id
)

SELECT
    l.snapshot_date,
    l.product_id, p.product_name, p.brand, p.category_name, p.department,
    l.warehouse_id, w.warehouse_name, w.country_code AS warehouse_country, w.region,
    l.available_quantity, l.reserved_quantity, l.on_hand_quantity, l.reorder_threshold,
    l.is_stockout, l.is_low_stock, l.inventory_value_usd,
    ROUND(COALESCE(d.daily_units, 0) / s.stocking_sites, 3)                       AS daily_demand_units,
    ROUND(l.available_quantity / NULLIF(COALESCE(d.daily_units, 0) / s.stocking_sites, 0), 1) AS days_of_cover,
    CASE WHEN l.available_quantity <= l.reorder_threshold
         THEN GREATEST(2 * l.reorder_threshold - l.available_quantity, 0) ELSE 0 END AS replenishment_units,
    CASE WHEN l.is_stockout THEN 'Stock-out'
         WHEN l.is_low_stock THEN 'Low stock'
         WHEN l.available_quantity / NULLIF(COALESCE(d.daily_units, 0) / s.stocking_sites, 0) > 180 THEN 'Overstock'
         ELSE 'Healthy' END                                                        AS stock_status,
    ROUND(COALESCE(d.daily_units, 0) * 365 / NULLIF(SUM(l.on_hand_quantity) OVER (PARTITION BY l.product_id), 0), 2)
                                                                                   AS annualised_turnover,
    ROUND(wl.on_hand_units / w.capacity_units, 4)                                  AS warehouse_utilization
FROM latest l
INNER JOIN sites s ON s.product_id = l.product_id
LEFT JOIN demand d ON d.product_id = l.product_id
LEFT JOIN dim_product p ON p.product_id = l.product_id AND p.is_current
LEFT JOIN dim_warehouse w ON w.warehouse_id = l.warehouse_id
LEFT JOIN wh_load wl ON wl.warehouse_id = l.warehouse_id
