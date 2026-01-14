# Data Generation Scripts

## Overview
These scripts generate test data for the procurement pipeline following the centralized replenishment model.

## ✅ Centralized Replenishment Model

This project uses a **centralized procurement system**:

### CORRECT: One Rule Per Product
```python
# generate_replenishment_rules.py
for product in products:
    rule = {
        'product_id': product['product_id'],
        'safety_stock': 50,
        'min_order_quantity': 12,
        # NO warehouse_id field
    }
```

**Result:** 100 rules (one per product) ✅

### ❌ WRONG: Rules Per Product-Warehouse
```python
# DON'T DO THIS!
for product in products:
    for warehouse in warehouses:
        rule = {
            'product_id': product['product_id'],
            'warehouse_id': warehouse['warehouse_id'],  # ❌ WRONG
            'safety_stock': 50,
        }
```

**Result:** 300 rules (100 products × 3 warehouses) → **Breaks the pipeline!**

**Why it breaks:**
- Joins explode: One SKU matches 3 rules → 3× rows
- Net demand multiplied incorrectly
- Supplier orders become 3× too large

## 📁 Files

### Master Data Generation
- **generate_suppliers.py** - Creates 10 suppliers
- **generate_warehouses.py** - Creates 3 warehouses (WH01, WH02, WH03)
- **generate_products.py** - Creates 100 products with SKUs
- **generate_replenishment_rules.py** - Creates 100 centralized rules ✅

### Operational Data Generation
- **generate_orders.py** - Creates daily POS orders (JSONL format)
- **generate_stock.py** - Creates warehouse stock snapshots (JSONL format)

### Orchestration
- **generate_all_data.py** - Runs all generation scripts in order

## 🔄 Stock Aggregation

Stock files contain warehouse dimension for realism, but are aggregated before net demand calculation:

### Raw Stock Files
```json
// WH01_2026-01-11.json (JSONL format)
{"warehouse_code": "WH01", "sku": "SKU00001", "available_stock": 100, "reserved_stock": 10}
{"warehouse_code": "WH01", "sku": "SKU00002", "available_stock": 200, "reserved_stock": 20}

// WH02_2026-01-11.json (JSONL format)
{"warehouse_code": "WH02", "sku": "SKU00001", "available_stock": 150, "reserved_stock": 5}
{"warehouse_code": "WH02", "sku": "SKU00002", "available_stock": 180, "reserved_stock": 15}
```

### Aggregation Query
```sql
-- This happens in the pipeline
SELECT 
  sku,
  SUM(available_stock) AS available_stock,
  SUM(reserved_stock) AS reserved_stock
FROM hive.default.stock
WHERE snapshot_date = '2026-01-11'
GROUP BY sku

-- Result:
-- SKU00001: available=250, reserved=15
-- SKU00002: available=380, reserved=35
```

## 📐 Database Schema

### replenishment_rules (centralized)
```sql
CREATE TABLE replenishment_rules (
    id SERIAL PRIMARY KEY,
    product_id INTEGER UNIQUE,  -- ← UNIQUE per product (centralized)
    safety_stock INTEGER,
    min_order_quantity INTEGER,
    pack_size INTEGER,
    case_size INTEGER,
    max_order_quantity INTEGER,
    reorder_point INTEGER
);
```

**Key constraints:**
- `product_id` is UNIQUE (one rule per product) ✅
- **No** `warehouse_id` column
- **No** composite key `(product_id, warehouse_id)`

## 📝 Data Format

All operational data files use **JSONL format** (newline-delimited JSON) for Hive compatibility:

```json
{"order_id": "ORD001", "sku": "SKU00001", "quantity": 5}
{"order_id": "ORD002", "sku": "SKU00002", "quantity": 3}
```

**Not** JSON arrays:
```json
// ❌ WRONG - Causes Hive "Start token not found" errors
[
  {"order_id": "ORD001", "sku": "SKU00001", "quantity": 5},
  {"order_id": "ORD002", "sku": "SKU00002", "quantity": 3}
]
```

## 🚀 Usage

### Generate All Data
```bash
python scripts/data_generation/generate_all_data.py
```

### Generate Specific Data
```bash
# Master data
python scripts/data_generation/generate_suppliers.py
python scripts/data_generation/generate_products.py
python scripts/data_generation/generate_replenishment_rules.py

# Operational data
python scripts/data_generation/generate_orders.py
python scripts/data_generation/generate_stock.py
```

## 🎯 Expected Counts

After running `generate_all_data.py`:

**Master Data:**
- Suppliers: 10
- Products: 100
- Warehouses: 3
- Replenishment Rules: **100** (one per product) ✅
- Supplier-Product Mappings: ~150

**Operational Data (per day):**
- Order line items: ~1,500-2,000 (5 POS stores × 50-200 orders × 1-5 items)
- Stock records: 300 (100 SKUs × 3 warehouses)
- Unique SKUs ordered: ~80-90

## ⚠️ Common Mistakes

1. **Creating product-warehouse rules** → Use centralized rules (one per product)
2. **Using JSON arrays instead of JSONL** → Each line must be a separate JSON object
3. **Not aggregating stock before net demand** → Must SUM across warehouses first
4. **Joining rules without deduplication** → Ensure product_id is unique in rules table

## 📚 Reference

For complete documentation, see:
- `database/schema.sql` - Database schema definitions
- `README.md` - Project documentation
- `VERIFICATION_REPORT.md` - Data generation verification
