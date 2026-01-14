"""Generate warehouse stock snapshots."""

import sys
from pathlib import Path
from datetime import datetime
import random
import json

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.config import RAW_STOCK_DIR
from database.db_connection import DatabaseConnection


def generate_stock_snapshot(snapshot_date: str):
    """Generate end-of-day stock levels for all warehouses."""
    # Get active products and warehouses
    products = DatabaseConnection.execute_query(
        "SELECT sku FROM products WHERE is_active = TRUE;",
        fetch=True
    )
    warehouses = DatabaseConnection.execute_query(
        "SELECT warehouse_code FROM warehouses WHERE is_active = TRUE;",
        fetch=True
    )
    
    if not products or not warehouses:
        print("No products or warehouses found. Please generate them first.")
        return
    
    # Create date directory
    date_dir = RAW_STOCK_DIR / snapshot_date
    date_dir.mkdir(parents=True, exist_ok=True)
    
    total_records = 0
    
    for warehouse in warehouses:
        warehouse_code = warehouse['warehouse_code']
        stock_data = []
        
        for product in products:
            # Generate realistic stock levels
            available_stock = random.randint(0, 500)
            reserved_stock = random.randint(0, min(available_stock, 50))
            
            stock_record = {
                "warehouse_code": warehouse_code,
                "sku": product['sku'],
                "available_stock": available_stock,
                "reserved_stock": reserved_stock,
                "snapshot_date": snapshot_date
            }
            stock_data.append(stock_record)
        
        # Save to JSONL file (one JSON object per line for Hive compatibility)
        output_file = date_dir / f"{warehouse_code}_{snapshot_date}.json"
        with open(output_file, 'w') as f:
            for record in stock_data:
                json.dump(record, f)
                f.write('\n')  # Each JSON object on its own line
        
        total_records += len(stock_data)
        print(f"  ✓ Generated {len(stock_data)} stock records for {warehouse_code}")
    
    print(f"✓ Total: {total_records} stock records across {len(warehouses)} warehouses")


if __name__ == "__main__":
    from datetime import date
    
    # Generate for today
    today = date.today().strftime("%Y-%m-%d")
    
    print(f"Generating stock snapshot for date: {today}")
    generate_stock_snapshot(today)
    print("Stock snapshot generation complete!")
