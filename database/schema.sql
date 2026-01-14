-- Procurement Pipeline Database Schema
-- PostgreSQL Schema for Master Data

-- Drop existing tables if they exist
DROP TABLE IF EXISTS replenishment_rules CASCADE;
DROP TABLE IF EXISTS supplier_products CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS warehouses CASCADE;

-- Suppliers table
CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_code VARCHAR(50) UNIQUE NOT NULL,
    supplier_name VARCHAR(200) NOT NULL,
    contact_email VARCHAR(100),
    contact_phone VARCHAR(50),
    lead_time_days INTEGER DEFAULT 2,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_suppliers_code ON suppliers(supplier_code);
CREATE INDEX idx_suppliers_active ON suppliers(is_active);

-- Warehouses table
CREATE TABLE warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_code VARCHAR(50) UNIQUE NOT NULL,
    warehouse_name VARCHAR(200) NOT NULL,
    location VARCHAR(200),
    capacity_cubic_meters DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_warehouses_code ON warehouses(warehouse_code);
CREATE INDEX idx_warehouses_active ON warehouses(is_active);

-- Products table (SKU master data)
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(300) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price DECIMAL(10, 2) NOT NULL,
    unit_of_measure VARCHAR(20) DEFAULT 'UNIT',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_active ON products(is_active);

-- Supplier-Product mapping (which supplier provides which product)
CREATE TABLE supplier_products (
    id SERIAL PRIMARY KEY,
    supplier_id INTEGER REFERENCES suppliers(supplier_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
    is_primary_supplier BOOLEAN DEFAULT FALSE,
    supplier_sku VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(supplier_id, product_id)
);

CREATE INDEX idx_supplier_products_supplier ON supplier_products(supplier_id);
CREATE INDEX idx_supplier_products_product ON supplier_products(product_id);
CREATE INDEX idx_supplier_products_primary ON supplier_products(is_primary_supplier);

-- Replenishment rules (centralized procurement constraints per product)
-- Note: This is a centralized system - one rule per SKU globally, not per warehouse
CREATE TABLE replenishment_rules (
    id SERIAL PRIMARY KEY,
    product_id INTEGER UNIQUE REFERENCES products(product_id) ON DELETE CASCADE,
    safety_stock INTEGER DEFAULT 10,
    min_order_quantity INTEGER DEFAULT 1,
    pack_size INTEGER DEFAULT 1,
    case_size INTEGER DEFAULT 1,
    max_order_quantity INTEGER,
    reorder_point INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_replenishment_rules_product ON replenishment_rules(product_id);

-- Comments for documentation
COMMENT ON TABLE suppliers IS 'Vendor information and contact details';
COMMENT ON TABLE warehouses IS 'Distribution centers and storage facilities';
COMMENT ON TABLE products IS 'Product master data (SKU catalog)';
COMMENT ON TABLE supplier_products IS 'Mapping between suppliers and products they provide';
COMMENT ON TABLE replenishment_rules IS 'Centralized procurement constraints per SKU (not per warehouse)';

COMMENT ON COLUMN replenishment_rules.safety_stock IS 'Minimum stock level to maintain';
COMMENT ON COLUMN replenishment_rules.min_order_quantity IS 'Minimum quantity per order (MOQ)';
COMMENT ON COLUMN replenishment_rules.pack_size IS 'Items per pack';
COMMENT ON COLUMN replenishment_rules.case_size IS 'Packs per case';
COMMENT ON COLUMN replenishment_rules.reorder_point IS 'Stock level triggering reorder';
