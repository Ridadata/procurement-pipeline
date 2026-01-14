"""Generate test data for products using Faker."""

import sys
from pathlib import Path
from faker import Faker
import random

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.config import DATA_GEN_CONFIG
from database.db_connection import DatabaseConnection

fake = Faker()


# Product categories and examples
PRODUCT_CATEGORIES = {
    "Fresh Produce": ["Apples", "Bananas", "Tomatoes", "Lettuce", "Carrots", "Oranges"],
    "Dairy": ["Milk", "Cheese", "Yogurt", "Butter", "Cream"],
    "Bakery": ["Bread", "Croissants", "Bagels", "Muffins", "Cookies"],
    "Beverages": ["Orange Juice", "Water", "Soda", "Coffee", "Tea"],
    "Frozen": ["Ice Cream", "Frozen Pizza", "Frozen Vegetables", "Frozen Fish"],
    "Snacks": ["Chips", "Nuts", "Crackers", "Chocolate", "Candy"],
    "Meat": ["Chicken", "Beef", "Lamb", "Turkey"],
    "Cleaning": ["Detergent", "Soap", "Bleach", "Sponges"],
    "Personal Care": ["Shampoo", "Toothpaste", "Deodorant", "Tissues"],
}


def generate_products(num_products: int = 100) -> list[dict]:
    """Generate fake product data."""
    products = []
    
    for i in range(num_products):
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        subcategory = random.choice(PRODUCT_CATEGORIES[category])
        
        product = {
            'sku': f"SKU{i+1:05d}",
            'product_name': f"{fake.company()} {subcategory}",
            'category': category,
            'subcategory': subcategory,
            'unit_price': round(random.uniform(0.5, 50.0), 2),
            'unit_of_measure': random.choice(["UNIT", "KG", "L", "PACK"]),
            'is_active': random.random() > 0.05  # 95% active
        }
        products.append(product)
    
    return products


def insert_products_to_db(products: list[dict]):
    """Insert products into the database."""
    query = """
        INSERT INTO products (sku, product_name, category, subcategory, 
                              unit_price, unit_of_measure, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sku) DO NOTHING;
    """
    
    data = [
        (p['sku'], p['product_name'], p['category'], p['subcategory'],
         p['unit_price'], p['unit_of_measure'], p['is_active'])
        for p in products
    ]
    
    DatabaseConnection.execute_many(query, data)
    print(f"✓ Inserted {len(products)} products")


def link_products_to_suppliers():
    """Create supplier-product mappings."""
    # Get all products and suppliers
    products = DatabaseConnection.execute_query("SELECT product_id FROM products;", fetch=True)
    suppliers = DatabaseConnection.execute_query("SELECT supplier_id FROM suppliers;", fetch=True)
    
    if not products or not suppliers:
        print("No products or suppliers found. Please generate them first.")
        return
    
    # Each product gets 1-2 suppliers
    mappings = []
    for product in products:
        num_suppliers_per_product = random.randint(1, 2)
        selected_suppliers = random.sample(suppliers, min(num_suppliers_per_product, len(suppliers)))
        
        for idx, supplier in enumerate(selected_suppliers):
            is_primary = (idx == 0)  # First one is primary
            mappings.append((supplier['supplier_id'], product['product_id'], is_primary))
    
    query = """
        INSERT INTO supplier_products (supplier_id, product_id, is_primary_supplier)
        VALUES (%s, %s, %s)
        ON CONFLICT (supplier_id, product_id) DO NOTHING;
    """
    
    DatabaseConnection.execute_many(query, mappings)
    print(f"✓ Created {len(mappings)} supplier-product mappings")


if __name__ == "__main__":
    num_products = DATA_GEN_CONFIG["num_products"]
    print(f"Generating {num_products} products...")
    
    products = generate_products(num_products)
    insert_products_to_db(products)
    
    print("\nLinking products to suppliers...")
    link_products_to_suppliers()
    
    print("Product generation complete!")
