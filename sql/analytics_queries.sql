"""SQL queries for Presto/Trino analytics."""

-- Query 1: Aggregated daily orders across all POS
-- This simulates what would run in Presto against HDFS data
SELECT 
    sku,
    SUM(quantity) as total_quantity,
    COUNT(DISTINCT order_id) as num_orders,
    COUNT(DISTINCT pos_store_id) as num_stores,
    order_date
FROM orders_raw
GROUP BY sku, order_date;


-- Query 2: Join orders with stock levels and master data
-- This demonstrates distributed join capabilities
SELECT 
    o.sku,
    o.total_quantity as demand,
    s.available_stock,
    s.reserved_stock,
    (s.available_stock - s.reserved_stock) as free_stock,
    p.product_name,
    p.category,
    p.unit_price
FROM aggregated_orders o
JOIN stock_snapshot s ON o.sku = s.sku
JOIN products p ON o.sku = p.sku
WHERE o.order_date = CURRENT_DATE;


-- Query 3: Net demand calculation
-- Complex calculation across multiple data sources
SELECT 
    o.sku,
    s.warehouse_code,
    o.total_quantity as aggregated_demand,
    s.available_stock,
    s.reserved_stock,
    rr.safety_stock,
    GREATEST(0, 
        o.total_quantity + rr.safety_stock - (s.available_stock - s.reserved_stock)
    ) as net_demand,
    rr.case_size,
    rr.min_order_quantity
FROM aggregated_orders o
CROSS JOIN warehouses w
JOIN stock_snapshot s ON o.sku = s.sku AND w.warehouse_code = s.warehouse_code
JOIN products p ON o.sku = p.sku
JOIN replenishment_rules rr ON p.product_id = rr.product_id 
    AND w.warehouse_id = rr.warehouse_id
WHERE o.order_date = CURRENT_DATE;


-- Query 4: Top products by demand
SELECT 
    p.product_name,
    p.category,
    SUM(o.quantity) as total_demand,
    COUNT(DISTINCT o.pos_store_id) as stores_ordering,
    AVG(o.quantity) as avg_order_size
FROM orders_raw o
JOIN products p ON o.sku = p.sku
WHERE o.order_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY p.product_name, p.category
ORDER BY total_demand DESC
LIMIT 20;


-- Query 5: Supplier order summary
-- Aggregate orders by supplier with totals
SELECT 
    sup.supplier_code,
    sup.supplier_name,
    COUNT(DISTINCT nd.sku) as num_skus,
    SUM(nd.adjusted_order_quantity) as total_quantity,
    SUM(nd.adjusted_order_quantity * p.unit_price) as total_value
FROM net_demand nd
JOIN products p ON nd.sku = p.sku
JOIN supplier_products sp ON p.product_id = sp.product_id
JOIN suppliers sup ON sp.supplier_id = sup.supplier_id
WHERE sp.is_primary_supplier = TRUE
  AND nd.adjusted_order_quantity > 0
GROUP BY sup.supplier_code, sup.supplier_name
ORDER BY total_value DESC;
