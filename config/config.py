"""Configuration management for the procurement pipeline."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

# HDFS-like directory structure
RAW_ORDERS_DIR = DATA_DIR / "raw" / "orders"
RAW_STOCK_DIR = DATA_DIR / "raw" / "stock"
PROCESSED_ORDERS_DIR = DATA_DIR / "processed" / "aggregated_orders"
PROCESSED_DEMAND_DIR = DATA_DIR / "processed" / "net_demand"
OUTPUT_DIR = DATA_DIR / "output" / "supplier_orders"
LOGS_DIR = DATA_DIR / "logs" / "exceptions"

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "procurement_db"),
    "user": os.getenv("DB_USER", "procurement"),
    "password": os.getenv("DB_PASSWORD", "procurement123"),
}

# Data generation settings
DATA_GEN_CONFIG = {
    "num_products": int(os.getenv("NUM_PRODUCTS", 100)),
    "num_warehouses": int(os.getenv("NUM_WAREHOUSES", 3)),
    "num_pos_stores": int(os.getenv("NUM_POS_STORES", 5)),
    "num_suppliers": int(os.getenv("NUM_SUPPLIERS", 10)),
}

# Pipeline settings
PIPELINE_CONFIG = {
    "date_format": os.getenv("DATE_FORMAT", "%Y-%m-%d"),
    "batch_execution_time": os.getenv("BATCH_EXECUTION_TIME", "22:00"),
}

def ensure_directories():
    """Create all necessary directories if they don't exist."""
    directories = [
        RAW_ORDERS_DIR,
        RAW_STOCK_DIR,
        PROCESSED_ORDERS_DIR,
        PROCESSED_DEMAND_DIR,
        OUTPUT_DIR,
        LOGS_DIR,
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        # Create .gitkeep file
        gitkeep = directory / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()

if __name__ == "__main__":
    ensure_directories()
    print("Directory structure created successfully!")
