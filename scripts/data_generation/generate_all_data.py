"""Master script to generate all test data."""

import sys
import argparse
from pathlib import Path
from datetime import date

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.config import ensure_directories, DATA_GEN_CONFIG

# Import generation modules
from scripts.data_generation import (
    generate_suppliers,
    generate_warehouses,
    generate_products,
    generate_replenishment_rules,
    generate_orders,
    generate_stock
)


def generate_all_data(target_date=None):
    """Generate all test data in the correct order.
    
    Args:
        target_date: Date string in YYYY-MM-DD format. If None, uses today's date.
    """
    # Use provided date or default to today
    if target_date is None:
        target_date = date.today().strftime("%Y-%m-%d")
    
    print("=" * 60)
    print("PROCUREMENT PIPELINE - DATA GENERATION")
    print("=" * 60)
    
    # Ensure directory structure exists
    print("\n1. Setting up directory structure...")
    ensure_directories()
    
    # Generate master data
    print("\n2. Generating suppliers...")
    from scripts.data_generation.generate_suppliers import generate_suppliers, insert_suppliers_to_db
    suppliers = generate_suppliers(DATA_GEN_CONFIG["num_suppliers"])
    insert_suppliers_to_db(suppliers)
    
    print("\n3. Generating warehouses...")
    from scripts.data_generation.generate_warehouses import generate_warehouses, insert_warehouses_to_db
    warehouses = generate_warehouses(DATA_GEN_CONFIG["num_warehouses"])
    insert_warehouses_to_db(warehouses)
    
    print("\n4. Generating products...")
    from scripts.data_generation.generate_products import generate_products, insert_products_to_db, link_products_to_suppliers
    products = generate_products(DATA_GEN_CONFIG["num_products"])
    insert_products_to_db(products)
    link_products_to_suppliers()
    
    print("\n5. Generating replenishment rules...")
    from scripts.data_generation.generate_replenishment_rules import generate_replenishment_rules
    generate_replenishment_rules()
    
    # Generate operational data for the target date
    print(f"\n6. Generating daily orders for {target_date}...")
    generate_orders.generate_daily_orders(target_date)
    
    print(f"\n7. Generating stock snapshot for {target_date}...")
    generate_stock.generate_stock_snapshot(target_date)
    
    print("\n" + "=" * 60)
    print("✓ ALL DATA GENERATION COMPLETE!")
    print("=" * 60)
    print(f"\nData generated for date: {target_date}")
    print("\nNext steps:")
    print(f"  1. Review the generated data in the 'data/raw/' directory")
    print(f"  2. Upload to HDFS: python scripts/upload_to_hdfs.py --date {target_date}")
    print(f"  3. Run the pipeline: python src/run_pipeline.py --date {target_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate test data for procurement pipeline')
    parser.add_argument('--date', type=str, help='Date in YYYY-MM-DD format (default: today)')
    args = parser.parse_args()
    
    try:
        generate_all_data(args.date)
    except Exception as e:
        print(f"\n✗ Error during data generation: {e}")
        import traceback
        traceback.print_exc()
