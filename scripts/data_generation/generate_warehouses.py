"""Generate test data for warehouses using Faker."""

import sys
from pathlib import Path
from faker import Faker
import random

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.config import DATA_GEN_CONFIG
from database.db_connection import DatabaseConnection

fake = Faker()


def generate_warehouses(num_warehouses: int = 3) -> list[dict]:
    """Generate fake warehouse data."""
    warehouses = []
    
    warehouse_locations = [
        "Casablanca Central", "Rabat North", "Tangier Port",
        "Marrakech Industrial", "Fes Distribution Center"
    ]
    
    for i in range(num_warehouses):
        warehouse = {
            'warehouse_code': f"WH{i+1:02d}",
            'warehouse_name': f"Warehouse {warehouse_locations[i % len(warehouse_locations)]}",
            'location': fake.address().replace('\n', ', ')[:200],
            'capacity_cubic_meters': round(random.uniform(5000, 50000), 2),
            'is_active': True
        }
        warehouses.append(warehouse)
    
    return warehouses


def insert_warehouses_to_db(warehouses: list[dict]):
    """Insert warehouses into the database."""
    query = """
        INSERT INTO warehouses (warehouse_code, warehouse_name, location, 
                                capacity_cubic_meters, is_active)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (warehouse_code) DO NOTHING;
    """
    
    data = [
        (w['warehouse_code'], w['warehouse_name'], w['location'],
         w['capacity_cubic_meters'], w['is_active'])
        for w in warehouses
    ]
    
    DatabaseConnection.execute_many(query, data)
    print(f"✓ Inserted {len(warehouses)} warehouses")


if __name__ == "__main__":
    num_warehouses = DATA_GEN_CONFIG["num_warehouses"]
    print(f"Generating {num_warehouses} warehouses...")
    
    warehouses = generate_warehouses(num_warehouses)
    insert_warehouses_to_db(warehouses)
    
    print("Warehouse generation complete!")
