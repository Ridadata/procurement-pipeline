"""Generate centralized replenishment rules (one per product)."""

import sys
from pathlib import Path
import random

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from database.db_connection import DatabaseConnection


def generate_replenishment_rules():
    """Generate centralized replenishment rules (one per product).
    
    This is a centralized procurement system:
    - One rule per SKU globally (not per warehouse)
    - Stock is aggregated across all warehouses
    - Net demand is calculated per SKU centrally
    """
    # Get all products
    products = DatabaseConnection.execute_query(
        "SELECT product_id FROM products WHERE is_active = TRUE;", 
        fetch=True
    )
    
    if not products:
        print("No products found. Please generate products first.")
        return
    
    rules = []
    # Create ONE rule per product (centralized model)
    for product in products:
        # Generate realistic procurement constraints
        case_size = random.choice([6, 12, 24])
        pack_size = random.choice([1, 4, 6])
        
        rule = (
            product['product_id'],
            random.randint(10, 100),  # safety_stock
            random.choice([1, case_size, case_size * 2]),  # min_order_quantity
            pack_size,
            case_size,
            random.randint(500, 2000),  # max_order_quantity
            random.randint(50, 200),  # reorder_point
        )
        rules.append(rule)
    
    query = """
        INSERT INTO replenishment_rules 
        (product_id, safety_stock, min_order_quantity, 
         pack_size, case_size, max_order_quantity, reorder_point)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """
    
    DatabaseConnection.execute_many(query, rules)
    print(f"✓ Created {len(rules)} replenishment rules (one per product - centralized model)")


if __name__ == "__main__":
    print("Generating replenishment rules...")
    generate_replenishment_rules()
    print("Replenishment rules generation complete!")
