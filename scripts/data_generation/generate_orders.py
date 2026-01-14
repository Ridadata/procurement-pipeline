"""Generate daily customer orders (POS data)."""

import sys
from pathlib import Path
from datetime import datetime
import random
import json

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.config import DATA_GEN_CONFIG, RAW_ORDERS_DIR
from database.db_connection import DatabaseConnection

fake_orders_per_store = lambda: random.randint(50, 200)


def generate_daily_orders(order_date: str, num_stores: int = 5):
    """Generate orders for all POS stores for a given date."""
    # Get active products
    products = DatabaseConnection.execute_query(
        "SELECT sku, unit_price FROM products WHERE is_active = TRUE;",
        fetch=True
    )
    
    if not products:
        print("No products found. Please generate products first.")
        return
    
    # Create date directory
    date_dir = RAW_ORDERS_DIR / order_date
    date_dir.mkdir(parents=True, exist_ok=True)
    
    total_orders = 0
    
    for store_id in range(1, num_stores + 1):
        store_code = f"POS{store_id:03d}"
        orders = []
        
        num_orders = fake_orders_per_store()
        
        for order_num in range(num_orders):
            order_id = f"ORD{order_date.replace('-', '')}{store_id:03d}{order_num:05d}"
            
            # Each order has 1-5 items
            num_items = random.randint(1, 5)
            selected_products = random.sample(products, min(num_items, len(products)))
            
            for product in selected_products:
                order = {
                    "order_id": order_id,
                    "pos_store_id": store_code,
                    "sku": product['sku'],
                    "quantity": random.randint(1, 10),
                    "order_date": order_date,
                    "unit_price": float(product['unit_price'])
                }
                orders.append(order)
        
        # Save to JSONL file (one JSON object per line for Hive compatibility)
        output_file = date_dir / f"{store_code}_{order_date}.json"
        with open(output_file, 'w') as f:
            for order in orders:
                json.dump(order, f)
                f.write('\n')  # Each JSON object on its own line
        
        total_orders += len(orders)
        print(f"  ✓ Generated {len(orders)} order items for {store_code}")
    
    print(f"✓ Total: {total_orders} order items across {num_stores} stores")


if __name__ == "__main__":
    from datetime import date
    
    # Generate for today
    today = date.today().strftime("%Y-%m-%d")
    num_stores = DATA_GEN_CONFIG["num_pos_stores"]
    
    print(f"Generating orders for date: {today}")
    generate_daily_orders(today, num_stores)
    print("Order generation complete!")
