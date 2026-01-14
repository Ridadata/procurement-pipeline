# 🏭 Procurement Data Pipeline

[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Hadoop](https://img.shields.io/badge/Hadoop-3.2.1-FF9900?style=flat&logo=apache&logoColor=white)](https://hadoop.apache.org/)
[![Presto](https://img.shields.io/badge/Presto-SQL-5890FF?style=flat&logo=presto&logoColor=white)](https://prestodb.io/)
[![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)

**Big Data project for centralized procurement using Hadoop, Presto, and Apache Airflow**

A production-ready data pipeline that processes Point-of-Sale (POS) orders and warehouse stock data to generate optimized supplier orders. Built with Hadoop HDFS for distributed storage, Presto for federated queries, PostgreSQL for master data, and Apache Airflow for orchestration.

---

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Technologies](#-technologies)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Usage Guide](#-usage-guide)
- [Data Flow](#-data-flow)
- [Pipeline Details](#-pipeline-details)
- [Monitoring & Access](#-monitoring--access)
- [Analysis Tools](#-analysis-tools)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

---

## ✨ Features

- **🔄 Centralized Procurement**: Single replenishment rule per SKU across all warehouses
- **📊 Big Data Storage**: HDFS-based data lake for scalable storage
- **🚀 Federated Queries**: Presto joins HDFS data with PostgreSQL master data
- **⏰ Batch Orchestration**: Apache Airflow DAG scheduling (daily at 22:00)
- **✅ Data Quality**: Automated validation checks (supplier mapping, demand spikes, duplicates)
- **📈 Exception Logging**: Comprehensive fault tracking and reporting
- **🐳 Fully Dockerized**: One-command deployment with 9 containerized services
- **📁 Historical Audit**: Date-partitioned data preservation

---

## 🏗 Architecture

## 🏗 Architecture

### System Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
├─────────────────┬───────────────────┬──────────────────────────────┤
│  POS Systems    │  Warehouse Stock  │  PostgreSQL Master Data      │
│  (5 stores)     │  (3 warehouses)   │  • Products (100 SKUs)       │
│  Daily Orders   │  Daily Snapshots  │  • Suppliers (10)            │
└────────┬────────┴────────┬──────────┴──────────┬───────────────────┘
         │                  │                      │
         ▼                  ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER                                    │
├────────────────────────────────┬────────────────────────────────────┤
│  HDFS Data Lake                │  PostgreSQL OLTP                   │
│  • /raw/orders/YYYY-MM-DD/    │  • products                        │
│  • /raw/stock/YYYY-MM-DD/     │  • suppliers                       │
│  • /processed/                 │  • warehouses                      │
│  • /output/                    │  • replenishment_rules             │
│  • /logs/                      │  • supplier_products               │
└────────────┬───────────────────┴───────────────┬────────────────────┘
             │                                    │
             └────────────────┬───────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    QUERY ENGINE                                      │
│  Presto (Distributed SQL)                                            │
│  • Hive Connector → HDFS                                             │
│  • PostgreSQL Connector → Master Data                                │
│  • Federated JOIN across data sources                                │
└────────────────────────────┬────────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 ORCHESTRATION LAYER                                  │
│  Apache Airflow                                                      │
│  • Scheduled DAG (daily 22:00)                                       │
│  • 6 Task Pipeline                                                   │
│  • Retries & Alerts                                                  │
└────────────────────────────┬────────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      OUTPUT                                          │
│  Supplier Orders (CSV)                                               │
│  • /data/output/supplier_orders/                                     │
│  • HDFS: /procurement/output/supplier_orders/                        │
└─────────────────────────────────────────────────────────────────────┘
```

### Docker Services (9 containers)

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| **PostgreSQL** | `procurement_postgres` | 5433 | Master data (products, suppliers, rules) |
| **HDFS NameNode** | `namenode` | 9870, 9000 | HDFS metadata & coordination |
| **HDFS DataNode** | `datanode` | - | HDFS block storage |
| **HDFS Client** | `hadoop_client` | - | CLI access with volume mounts |
| **Presto** | `presto` | 8080 | Distributed SQL query engine |
| **Airflow DB** | `airflow_postgres` | 5434 | Airflow metadata storage |
| **Airflow Webserver** | `airflow_webserver` | 8081 | Web UI (admin/admin) |
| **Airflow Scheduler** | `airflow_scheduler` | - | Task scheduling & execution |
| **Airflow Init** | `airflow_init` | - | One-time DB setup (exits) |

---

## 🛠 Technologies

### Storage & Processing
- **Hadoop HDFS 3.2.1** - Distributed file system for raw/processed data
- **Presto** - Federated SQL query engine (Hive + PostgreSQL connectors)
- **PostgreSQL 15** - OLTP database for master data
- **PostgreSQL 13** - Airflow metadata storage

### Orchestration & Monitoring
- **Apache Airflow 2.x** - Workflow orchestration with Web UI
- **Docker Compose** - Multi-container deployment

### Development
- **Python 3.9+** - Data generation, pipeline scripts
- **psycopg2** - PostgreSQL adapter
- **hdfs dfs** - Hadoop CLI commands

---

## 📁 Project Structure

## 📁 Project Structure

```
procurement-pipeline/
├── 📂 airflow/                          # Apache Airflow orchestration
│   ├── dags/
│   │   └── procurement_pipeline_dag.py  # Main DAG (6 tasks, daily @ 22:00)
│   ├── logs/                            # Task execution logs
│   └── plugins/                         # Custom Airflow plugins
│
├── 📂 scripts/                          # Utility scripts
│   ├── data_generation/                 # Test data generators
│   │   ├── generate_all_data.py         # Master script (--date param)
│   │   ├── generate_suppliers.py        # 10 suppliers
│   │   ├── generate_products.py         # 100 products
│   │   ├── generate_warehouses.py       # 3 warehouses
│   │   ├── generate_orders.py           # POS orders (5 stores)
│   │   ├── generate_stock.py            # Warehouse stock snapshots
│   │   └── generate_replenishment_rules.py  # 100 centralized rules
│   ├── upload_to_hdfs.py                # HDFS upload automation
│   ├── analyze_data.py                  # Data overview & testing tool
│   ├── setup_hdfs.sh                    # HDFS directory initialization
│   └── setup_hdfs.ps1                   # Windows version
│
├── 📂 docker/                           # Docker infrastructure
│   ├── docker-compose.yml               # All 9 services in one file
│   └── presto/
│       └── catalog/
│           ├── hive.properties          # HDFS connector config
│           └── postgresql.properties    # PostgreSQL connector config
│
├── 📂 database/                         # Database schemas
│   ├── schema.sql                       # PostgreSQL tables DDL
│   └── db_connection.py                 # Connection utilities
│
├── 📂 config/                           # Configuration
│   ├── config.py                        # Database & HDFS configs
│   └── .env.example                     # Environment variables template
│
├── 📂 data/                             # Local data directory (gitignored)
│   ├── raw/
│   │   ├── orders/YYYY-MM-DD/           # POS order JSONL files
│   │   └── stock/YYYY-MM-DD/            # Stock snapshot JSONL files
│   ├── processed/                       # Intermediate results
│   ├── output/
│   │   └── supplier_orders/             # Final CSV outputs
│   └── logs/                            # Pipeline execution logs
│
├── 📂 sql/                              # SQL queries
│   └── analytics_queries.sql            # Sample analytical queries
│
├── 📄 requirements.txt                  # Python dependencies
├── 📄 .gitignore                        # Git exclusions
├── 📄 README.md                         # This file
├── 📄 PROJECT_STRUCTURE.md              # Detailed documentation
└── 📄 PRE_COMMIT_CHECKLIST.md           # GitHub push checklist
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker** and **Docker Compose** installed
- **Python 3.9+** installed
- **8GB RAM** minimum (recommended: 16GB)
- **10GB disk space** for Docker volumes

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/procurement-pipeline.git
   cd procurement-pipeline
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   # Copy the example file
   cp .env.example .env
   
   # For Windows PowerShell:
   Copy-Item .env.example .env
   ```
   
   **Note:** Default values work for local development. For production, update passwords in `.env`

4. **Start all services**
   ```bash
   cd docker
   docker-compose up -d
   ```

5. **Wait for services to be ready** (~2 minutes)
   ```bash
   docker-compose ps
   # All services should show "healthy" or "Up"
   ```

6. **Initialize HDFS directories**
   ```bash
   cd ..
   # Linux/Mac:
   bash scripts/setup_hdfs.sh
   
   # Windows PowerShell:
   powershell scripts/setup_hdfs.ps1
   ```

✅ **Your environment is ready!**

---

## 📖 Usage Guide

### 1. Generate Test Data

Generate operational data for a specific date:

```bash
# Generate for today
python scripts/data_generation/generate_all_data.py

# Generate for specific date
python scripts/data_generation/generate_all_data.py --date 2026-01-14
```

**Output:**
- `data/raw/orders/2026-01-14/` - 5 POS store order files (JSONL)
- `data/raw/stock/2026-01-14/` - 3 warehouse stock snapshots (JSONL)

### 2. Upload Data to HDFS

```bash
# Upload today's data
python scripts/upload_to_hdfs.py

# Upload specific date
python scripts/upload_to_hdfs.py --date 2026-01-14
```

**HDFS Structure Created:**
```
/procurement/
├── raw/
│   ├── orders/2026-01-14/
│   │   ├── orders_store_001.json
│   │   ├── orders_store_002.json
│   │   └── ...
│   └── stock/2026-01-14/
│       ├── stock_WH001.json
│       └── ...
├── processed/
│   ├── aggregated_orders/
│   └── net_demand/
├── output/
│   └── supplier_orders/
└── logs/
    └── exceptions/
```

### 3. Run the Pipeline

**Option A: Apache Airflow (Recommended for production)**

1. Access Airflow UI: http://localhost:8081
2. Login: `admin` / `admin`
3. Enable the `procurement_pipeline` DAG
4. Trigger manually or wait for scheduled run (22:00 daily)

**Option B: Python Script (Quick testing)**

```bash
python airflow/dags/procurement_pipeline_dag.py
```

### 4. Analyze Results

```bash
# Use built-in analysis tool
python scripts/analyze_data.py
```

**Output displays:**
- ✅ PostgreSQL master data summary
- ✅ HDFS data overview (orders & stock)
- ✅ Combined analysis (top products with supplier info)

### 5. View Supplier Orders

```bash
# Local output
cat data/output/supplier_orders/supplier_orders_20260114.csv

# HDFS output
docker exec hadoop_client hdfs dfs -cat /procurement/output/supplier_orders/supplier_orders_20260114.csv
```

**CSV Format:**
```csv
supplier_code,supplier_name,sku,product_name,net_demand,case_size,order_quantity
SUP001,Tech Supplies Inc,SKU00001,Product A,45,12,48
SUP002,Office Goods Ltd,SKU00015,Product B,23,6,24
```

---

## 🔄 Data Flow

### Pipeline Execution Flow

```
┌──────────────────────────────────────────────────────────────────┐
│ Step 1: CHECK DATA AVAILABILITY                                  │
│ • Verify HDFS files exist for execution date                     │
│ • Count POS order files (expect 5)                               │
│ • Count warehouse stock files (expect 3)                         │
└───────────────────────┬──────────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 2: CREATE HIVE TABLES                                       │
│ • CREATE EXTERNAL TABLE orders (location: HDFS /raw/orders/)    │
│ • CREATE EXTERNAL TABLE stock (location: HDFS /raw/stock/)      │
│ • Format: JSON                                                   │
└───────────────────────┬──────────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 3: VALIDATE DATA QUALITY                                    │
│ • Check: All products have primary supplier mapping             │
│ • Check: No demand spikes >500% vs safety stock                 │
│ • Check: No zero available stock with positive reserved         │
│ • Check: No duplicate SKU records per warehouse                 │
└───────────────────────┬──────────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 4: CALCULATE NET DEMAND                                     │
│ • Aggregate orders by SKU across all stores                     │
│ • Join with stock levels by SKU (sum across warehouses)         │
│ • Formula: max(0, orders + safety_stock - free_stock)           │
│ • Export: aggregated_orders & net_demand to HDFS /processed/    │
└───────────────────────┬──────────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 5: GENERATE SUPPLIER ORDERS                                 │
│ • Join net_demand with replenishment_rules                      │
│ • Apply case rounding: CEIL(net_demand / case_size) * case_size│
│ • Enforce MOQ: max(order_qty, min_order_quantity)               │
│ • Filter: order_quantity > 0                                     │
│ • Export: CSV to /data/output/ and HDFS /output/                │
└───────────────────────┬──────────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│ Step 6: CLEANUP TEMP TABLES                                      │
│ • DROP TABLE IF EXISTS hive.default.orders                      │
│ • DROP TABLE IF EXISTS hive.default.stock                       │
│ • Log execution metrics                                          │
└──────────────────────────────────────────────────────────────────┘
```

### Net Demand Calculation Formula

```python
for each SKU:
    # Aggregate demand
    total_orders = SUM(quantity) FROM orders WHERE sku = ?
    
    # Aggregate stock
    total_available = SUM(available_stock) FROM stock WHERE sku = ?
    total_reserved = SUM(reserved_stock) FROM stock WHERE sku = ?
    free_stock = total_available - total_reserved
    
    # Get safety stock from replenishment rules
    safety_stock = SELECT safety_stock FROM rules WHERE sku = ?
    
    # Calculate net demand
    net_demand = MAX(0, total_orders + safety_stock - free_stock)
    
    # Apply case rounding
    case_size = SELECT case_size FROM rules WHERE sku = ?
    order_quantity = CEIL(net_demand / case_size) * case_size
    
    # Enforce MOQ
    min_order_qty = SELECT min_order_quantity FROM rules WHERE sku = ?
    final_order = MAX(order_quantity, min_order_qty) IF net_demand > 0
```

---

## 📊 Pipeline Details

### Airflow DAG Configuration

**Schedule:** `0 22 * * *` (Daily at 22:00 UTC)  
**Catchup:** `False` (no backfilling)  
**Retries:** 2 per task  
**Retry Delay:** 5 minutes

**Task Dependencies:**
```
check_data_availability
         ↓
create_hive_tables
         ↓
validate_data_quality
         ↓
calculate_net_demand
         ↓
generate_supplier_orders
         ↓
cleanup_temp_tables
```

### Data Quality Checks

| Check | Description | Threshold |
|-------|-------------|-----------|
| **Supplier Mapping** | All ordered products must have primary supplier | 0 unmapped |
| **Demand Spike** | Orders shouldn't exceed 5x safety stock | < 5% violations |
| **Stock Consistency** | No reserved stock when available = 0 | 0 violations |
| **Duplicate SKUs** | Each SKU appears once per warehouse snapshot | 0 duplicates |

---

## 🖥 Monitoring & Access

### Web Interfaces

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Airflow UI** | http://localhost:8081 | admin / admin | DAG monitoring, logs, task execution |
| **Presto UI** | http://localhost:8080 | - | Query monitoring, cluster status |
| **HDFS NameNode** | http://localhost:9870 | - | HDFS browser, cluster health |

### Database Connections

```bash
# PostgreSQL (Master Data)
docker exec -it procurement_postgres psql -U procurement -d procurement_db
# OR: localhost:5433 / procurement / procurement123

# PostgreSQL (Airflow Metadata)
docker exec -it airflow_postgres psql -U airflow -d airflow_db
# OR: localhost:5434 / airflow / airflow

# Presto CLI
docker exec -it presto presto-cli

# HDFS CLI
docker exec -it hadoop_client hdfs dfs -ls /procurement/
```

### Useful Commands

```bash
# View all containers
docker-compose ps

# View Airflow logs
docker-compose logs -f airflow_scheduler

# Check HDFS usage
docker exec hadoop_client hdfs dfs -du -h /procurement/

# Run Presto query
docker exec presto presto-cli --execute "SELECT COUNT(*) FROM hive.default.orders"

# View DAG execution history
docker exec airflow_webserver airflow dags list-runs -d procurement_pipeline

# Clear DAG run history
docker exec airflow_webserver airflow dags delete procurement_pipeline
```

---

## 🔍 Analysis Tools

### analyze_data.py

Quick data overview and system testing:

```bash
python scripts/analyze_data.py
```

**Output includes:**
1. **Master Data Summary**
   - Product, supplier, warehouse, rules counts
   
2. **HDFS Data Overview**
   - Latest data date available
   - Order items count & unique products
   - Stock records & availability

3. **Combined Analysis**
   - Top 10 products ordered (from HDFS)
   - Product details with suppliers (from PostgreSQL)

### SQL Analytics

Sample queries in `sql/analytics_queries.sql`:

```sql
-- Top products by order volume
SELECT sku, SUM(quantity) as total_qty
FROM hive.default.orders
GROUP BY sku
ORDER BY total_qty DESC
LIMIT 10;

-- Products with low stock
SELECT s.sku, s.available_stock, r.safety_stock
FROM hive.default.stock s
JOIN postgresql.public.replenishment_rules r ON s.sku = r.sku
WHERE s.available_stock < r.safety_stock;

-- Supplier order summary
SELECT supplier_code, COUNT(*) as products, SUM(order_quantity) as total_qty
FROM supplier_orders
GROUP BY supplier_code;
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. Services not starting

**Symptom:** Container exits immediately or shows `unhealthy`

**Solution:**
```bash
# Check logs
docker-compose logs namenode
docker-compose logs presto

# Restart specific service
docker-compose restart namenode

# Full reset
docker-compose down -v
docker-compose up -d
```

#### 2. Database connection error

**Symptom:** `psycopg2.OperationalError: could not connect`

**Solution:**
```bash
# Verify PostgreSQL is running
docker ps | grep postgres

# Test connection
docker exec procurement_postgres psql -U procurement -d procurement_db -c "SELECT 1"

# Check if port 5433 is available
netstat -an | grep 5433
```

#### 3. HDFS upload failed

**Symptom:** `hdfs: command not found` or `No such file or directory`

**Solution:**
```bash
# Check Hadoop containers
docker ps | grep hadoop

# Verify namenode is accessible
curl http://localhost:9870

# Check HDFS safemode
docker exec namenode hdfs dfsadmin -safemode get

# Leave safemode if stuck
docker exec namenode hdfs dfsadmin -safemode leave
```

#### 4. Presto query timeout

**Symptom:** `Query exceeded maximum time limit`

**Solution:**
```bash
# Check Presto is running
docker exec presto presto-cli --execute "SELECT 1"

# Verify Hive connector
docker exec presto cat /opt/presto-server/etc/catalog/hive.properties

# Restart Presto
docker-compose restart presto
```

#### 5. Airflow DAG not appearing

**Symptom:** DAG not visible in UI

**Solution:**
```bash
# Check DAG file syntax
python airflow/dags/procurement_pipeline_dag.py

# Refresh DAGs
docker exec airflow_scheduler airflow dags list

# Check scheduler logs
docker-compose logs -f airflow_scheduler
```

### Performance Tuning

```bash
# Increase Docker memory limit (Docker Desktop)
# Settings → Resources → Memory: 8GB minimum

# Increase Presto memory
# Edit docker/presto/config.properties:
# query.max-memory=2GB
# query.max-memory-per-node=1GB

# HDFS replication factor (single-node setup)
docker exec namenode hdfs dfs -setrep -w 1 /procurement/
```

---

## 🤝 Contributing

Contributions and suggestions are welcome!

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit: `git commit -m 'feat: add your feature'`
4. Push: `git push origin feature/your-feature`
5. Open a Pull Request

---

## 📝 License

Academic project - **Fondements Big Data**  
**Academic Year:** 2025-2026

---

## 📞 Contact

For questions or suggestions, open an issue on GitHub.

---

##  Key Learnings

-  Distributed Storage with Hadoop HDFS
-  Federated Queries with Presto (cross-system JOINs)
-  Workflow Orchestration with Apache Airflow
-  Data Quality Validation in production pipelines
-  Batch Processing patterns for Big Data
-  Containerization with Docker Compose
-  Data Lake Architecture (raw/processed/output zones)
-  OLTP + OLAP hybrid systems

---

##  Quick Reference

### Most Used Commands

```bash
# Start everything
cd docker && docker-compose up -d

# Generate & upload data
python scripts/data_generation/generate_all_data.py --date 2026-01-14
python scripts/upload_to_hdfs.py --date 2026-01-14

# Analyze results
python scripts/analyze_data.py

# Check HDFS
docker exec hadoop_client hdfs dfs -ls -R /procurement/

# Run Presto query
docker exec presto presto-cli --execute 'SELECT COUNT(*) FROM hive.default.orders'

# View Airflow logs
docker-compose -f docker/docker-compose.yml logs -f airflow_scheduler

# Stop everything
cd docker && docker-compose down
```

### Project Statistics

- **Docker Containers:** 9
- **Python Files:** ~15
- **SQL Queries:** ~20
- **Data Generators:** 6
- **HDFS Directories:** 7
- **PostgreSQL Tables:** 5
- **Airflow Tasks:** 6
- **Lines of Code:** ~2000+

---

##  Acknowledgments

- ENSA Al-Hoceima for the Big Data curriculum
- Apache Software Foundation for Hadoop, Presto, and Airflow
- PostgreSQL Global Development Group
- Docker Community

---

*Last Updated: January 14, 2026*
