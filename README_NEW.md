# Procurement Data Pipeline
**ENSA Al-Hoceima - Fondements Big Data**

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  POS Systems    │────▶│  HDFS Data Lake  │────▶│  Presto Query   │
│  (Daily Orders) │     │  /raw/orders/    │     │  Engine         │
└─────────────────┘     │  /raw/stock/     │     └────────┬────────┘
                        └──────────────────┘              │
                                                           │
┌─────────────────┐                                       │
│  PostgreSQL     │                                       │
│  (Master Data)  │◀──────────────────────────────────────┘
│  - Products     │              
│  - Suppliers    │              ┌─────────────────────┐
│  - Rules        │              │  Supplier Orders    │
└─────────────────┘              │  (CSV/JSON Output)  │
                                 └─────────────────────┘
```

## Project Structure

```
procurement-pipeline/
├── airflow/                    # Apache Airflow orchestration
│   ├── dags/
│   │   └── procurement_pipeline_dag.py   # Main DAG
│   └── docker-compose.yml      # Airflow services
│
├── src/                        # Source code
│   ├── data_generation/        # Data generation scripts
│   │   └── generate_test_data.py
│   └── utils/
│       └── hdfs_utils.py
│
├── docker/                     # Docker configuration
│   ├── docker-compose.yml      # All services
│   ├── presto/                 # Presto configuration
│   │   └── catalog/
│   │       ├── hive.properties
│   │       └── postgresql.properties
│   └── hadoop/                 # Hadoop configuration (if needed)
│
├── database/                   # Database schema
│   └── schema.sql
│
├── data/                       # Local data directory
│   ├── raw/
│   │   ├── orders/YYYY-MM-DD/
│   │   └── stock/YYYY-MM-DD/
│   └── output/
│       └── supplier_orders/
│
├── notebooks/                  # Jupyter notebooks (exploration)
│   └── 01_generate_data.ipynb
│
├── docs/                       # Documentation
│   └── ARCHITECTURE.md
│
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## Quick Start

### 1. Start Infrastructure

```bash
# Start all services (PostgreSQL, Hadoop, Presto)
docker-compose -f docker/docker-compose.yml up -d

# Verify services
docker ps
```

### 2. Initialize Database

```bash
# Database schema is auto-loaded on first start
docker exec procurement_postgres psql -U procurement -d procurement_db -c "\dt"
```

### 3. Generate Test Data

```bash
# Open Jupyter notebook
docker exec -it procurement_jupyter jupyter notebook list

# Or run Python script
python src/data_generation/generate_test_data.py
```

### 4. Run Pipeline with Airflow

```bash
# Start Airflow (see airflow/README.md)
cd airflow
docker-compose up -d

# Access UI: http://localhost:8080
# Trigger DAG: procurement_pipeline
```

### 5. Manual Pipeline Execution (Alternative)

```bash
python src/run_pipeline.py --date 2026-01-13
```

## Pipeline Steps

1. **Data Collection** - Ingest POS orders and stock snapshots from HDFS
2. **Data Validation** - Check data quality, detect anomalies
3. **Net Demand Calculation** - Formula: `max(0, orders + safety_stock - (available - reserved))`
4. **Supplier Order Generation** - Apply case rounding, MOQ rules
5. **Exception Reporting** - Log data quality issues

## Technologies

- **Storage**: HDFS (Hadoop 3.2.1)
- **Query Engine**: Presto (latest)
- **OLTP Database**: PostgreSQL 15
- **Orchestration**: Apache Airflow 2.x
- **Containers**: Docker & Docker Compose

## Data Flow

```
POS Files (JSON) → HDFS /raw/orders/
Stock Files (JSON) → HDFS /raw/stock/
                         ↓
              Presto External Tables
                         ↓
           JOIN with PostgreSQL master data
                         ↓
              Net Demand Calculation
                         ↓
            Supplier Orders (CSV output)
```

## Key Constraints

- ✅ Batch processing only (no streaming)
- ✅ Centralized procurement model (one rule per SKU)
- ✅ JSONL format for Hive compatibility
- ✅ End-of-day execution window (22:00)
- ✅ Historical data preserved for auditability

## Output

Daily supplier orders saved to:
```
data/output/supplier_orders/supplier_orders_YYYYMMDD.csv
```

Format:
```csv
supplier_code,supplier_name,sku,product_name,net_demand,case_size,order_quantity
SUP001,ABC Suppliers,SKU00001,Product A,45,12,48
```

## Monitoring

- **Presto UI**: http://localhost:8080
- **Airflow UI**: http://localhost:8081  
- **HDFS**: http://localhost:9870

## License

Academic project - ENSA Al-Hoceima
