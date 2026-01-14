"""Generate test data for suppliers using Faker."""

import sys
from pathlib import Path
from faker import Faker
import random

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.config import DATA_GEN_CONFIG
from database.db_connection import DatabaseConnection

fake = Faker()


def generate_suppliers(num_suppliers: int = 10) -> list[dict]:
    """Generate fake supplier data."""
    suppliers = []
    
    supplier_types = [
        "Fresh Foods", "Dairy Products", "Beverages", "Bakery",
        "Frozen Foods", "Cleaning Supplies", "Personal Care",
        "Snacks & Confectionery", "Meat & Seafood", "International Foods"
    ]
    
    for i in range(num_suppliers):
        supplier = {
            'supplier_code': f"SUP{i+1:03d}",
            'supplier_name': f"{fake.company()} {random.choice(supplier_types)}",
            'contact_email': fake.company_email(),
            'contact_phone': fake.phone_number()[:20],
            'lead_time_days': random.randint(1, 5),
            'is_active': True
        }
        suppliers.append(supplier)
    
    return suppliers


def insert_suppliers_to_db(suppliers: list[dict]):
    """Insert suppliers into the database."""
    query = """
        INSERT INTO suppliers (supplier_code, supplier_name, contact_email, 
                               contact_phone, lead_time_days, is_active)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (supplier_code) DO NOTHING;
    """
    
    data = [
        (s['supplier_code'], s['supplier_name'], s['contact_email'],
         s['contact_phone'], s['lead_time_days'], s['is_active'])
        for s in suppliers
    ]
    
    DatabaseConnection.execute_many(query, data)
    print(f"✓ Inserted {len(suppliers)} suppliers")


if __name__ == "__main__":
    num_suppliers = DATA_GEN_CONFIG["num_suppliers"]
    print(f"Generating {num_suppliers} suppliers...")
    
    suppliers = generate_suppliers(num_suppliers)
    insert_suppliers_to_db(suppliers)
    
    print("Supplier generation complete!")
