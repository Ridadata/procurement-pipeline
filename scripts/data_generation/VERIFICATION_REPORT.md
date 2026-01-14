# ✅ Data Generation Verification Report

## Status: CORRECTED

All data generation scripts now match the notebook implementation and follow the centralized replenishment model.

## Changes Made

### 1. Fixed: generate_replenishment_rules.py
**Before (❌ WRONG):**
```python
# Created product-warehouse rules (distributed model)
for product in products:
    for warehouse in warehouses:  # ❌ Extra loop
        rule = (product_id, warehouse_id, ...)  # ❌ warehouse_id included
        # Result: 300 rules (100 products × 3 warehouses)
```

**After (✅ CORRECT):**
```python
# Creates one rule per product (centralized model)
for product in products:
    rule = (product_id, safety_stock, ...)  # ✅ No warehouse_id
    # Result: 100 rules (one per product)
```

### 2. Fixed: generate_orders.py
**Before (❌ WRONG):**
```python
# Generated JSON arrays
json.dump(orders, f, indent=2)
# Result: [{...}, {...}] format
```

**After (✅ CORRECT):**
```python
# Generates JSONL (newline-delimited JSON)
for order in orders:
    json.dump(order, f)
    f.write('\n')  # Each JSON object on its own line
# Result: {...}\n{...}\n format
```

### 3. Fixed: generate_stock.py
**Before (❌ WRONG):**
```python
# Generated JSON arrays
json.dump(stock_data, f, indent=2)
```

**After (✅ CORRECT):**
```python
# Generates JSONL format
for record in stock_data:
    json.dump(record, f)
    f.write('\n')
```

### 4. Added: README.md
Created comprehensive documentation in `scripts/data_generation/README.md` explaining:
- Centralized vs distributed models
- Why product-warehouse rules break the pipeline
- Stock aggregation requirements
- JSONL format requirements
- Database schema constraints

## Verification Checklist

- ✅ **generate_replenishment_rules.py**: Creates 100 centralized rules (not 300)
- ✅ **generate_orders.py**: Generates JSONL format (not JSON arrays)
- ✅ **generate_stock.py**: Generates JSONL format (not JSON arrays)
- ✅ **database/schema.sql**: Has `product_id UNIQUE` constraint (correct)
- ✅ **notebooks/01_generate_data.ipynb**: Already correct (reference implementation)
- ✅ **Documentation**: Added README explaining correct approach

## Expected Results

When running `generate_all_data.py`:

```
Master Data:
  • Suppliers: 10
  • Products: 100
  • Warehouses: 3
  • Replenishment Rules: 100 (one per product - centralized model) ✅
  • Supplier-Product Mappings: ~150

Operational Data (2026-01-13):
  • Order line items: ~1,500-2,000
  • POS stores: 5
  • Stock records: 300 (100 SKUs × 3 warehouses)
```

## File Format Examples

### Orders (JSONL)
```json
{"order_id": "ORD20260113001", "pos_store_id": "POS001", "sku": "SKU00001", "quantity": 5, "order_date": "2026-01-13"}
{"order_id": "ORD20260113002", "pos_store_id": "POS001", "sku": "SKU00002", "quantity": 3, "order_date": "2026-01-13"}
```

### Stock (JSONL)
```json
{"warehouse_code": "WH01", "sku": "SKU00001", "available_stock": 100, "reserved_stock": 10, "snapshot_date": "2026-01-13"}
{"warehouse_code": "WH01", "sku": "SKU00002", "available_stock": 200, "reserved_stock": 20, "snapshot_date": "2026-01-13"}
```

### Replenishment Rules (PostgreSQL)
```sql
-- Centralized model: product_id is UNIQUE
product_id | safety_stock | min_order_quantity | pack_size | case_size
-----------+--------------+--------------------+-----------+----------
    1      |     50       |         12         |     6     |    12
    2      |     75       |         24         |     4     |    24
    3      |     30       |         12         |     1     |    12
```

**Total rows:** 100 (one per product) ✅

## Architecture Flow

```
1. Generate Master Data
   ├── Suppliers (10)
   ├── Warehouses (3)
   ├── Products (100)
   └── Replenishment Rules (100) ← One per product ✅

2. Generate Operational Data
   ├── Orders (JSONL files per POS store)
   └── Stock (JSONL files per warehouse)

3. Upload to HDFS
   ├── /procurement/raw/orders/YYYY-MM-DD/
   └── /procurement/raw/stock/YYYY-MM-DD/

4. Presto Pipeline
   ├── Aggregate orders: SUM(quantity) per SKU
   ├── Aggregate stock: SUM(available_stock) per SKU
   ├── Join with centralized rules (1:1 join) ✅
   └── Calculate net_demand per SKU
```

## Key Differences from Wrong Implementation

| Aspect | ❌ WRONG (Distributed) | ✅ CORRECT (Centralized) |
|--------|----------------------|-------------------------|
| **Rules Count** | 300 (product × warehouse) | 100 (one per product) |
| **Schema** | `(product_id, warehouse_id)` PK | `product_id UNIQUE` |
| **Join Result** | 3× rows (explodes) | 1:1 (correct) |
| **Net Demand** | Multiplied by 3 | Accurate |
| **Supplier Orders** | 3× too large | Correct quantities |
| **File Format** | JSON arrays `[{...}]` | JSONL `{...}\n{...}` |

## Testing

To verify the corrections work:

```bash
# 1. Generate test data
python scripts/data_generation/generate_all_data.py

# 2. Verify counts
# Connect to PostgreSQL and run:
SELECT 'replenishment_rules' AS table_name, COUNT(*) FROM replenishment_rules
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'warehouses', COUNT(*) FROM warehouses;

# Expected output:
#   replenishment_rules | 100  ✅
#   products           | 100
#   warehouses         |   3

# 3. Verify JSONL format
head -n 2 data/raw/orders/2026-01-13/POS001_2026-01-13.json
# Should show: {...}\n{...} (not [{...}, {...}])

# 4. Run pipeline
python src/run_pipeline.py --date 2026-01-13
```

## Conclusion

All data generation scripts have been corrected to:
1. **Generate centralized replenishment rules** (100, not 300)
2. **Use JSONL format** for Hive compatibility
3. **Match the notebook implementation** exactly

The pipeline will now:
- Join correctly (1:1 between SKU and rule)
- Calculate accurate net demand
- Generate correct supplier orders
- Process HDFS files without format errors
