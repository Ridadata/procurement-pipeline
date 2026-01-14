"""
Procurement Pipeline DAG - Apache Airflow
Batch processing: Daily supplier order generation

DAG runs end-of-day (22:00) to process:
1. Collect POS orders from HDFS
2. Load stock snapshots
3. Calculate net demand
4. Generate supplier orders
5. Handle exceptions
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import json

# Default arguments
default_args = {
    'owner': 'procurement_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'procurement_pipeline',
    default_args=default_args,
    description='Daily procurement pipeline - batch processing',
    schedule_interval='0 22 * * *',  # Run at 22:00 daily
    catchup=False,
    tags=['procurement', 'batch', 'supplier-orders'],
)

def initialize_presto_schema(**context):
    """Task 1: Initialize Presto Hive schema (idempotent)"""
    print("Initializing Presto Hive schema...")
    
    # Create default schema if not exists
    cmd = ['docker', 'exec', 'presto', 'presto-cli', '--execute', 
           'CREATE SCHEMA IF NOT EXISTS hive.default']
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Failed to create Hive schema: {result.stderr}")
    
    # Verify schema exists
    cmd_verify = ['docker', 'exec', 'presto', 'presto-cli', '--execute', 
                  'SHOW SCHEMAS FROM hive']
    result = subprocess.run(cmd_verify, capture_output=True, text=True)
    
    if 'default' not in result.stdout:
        raise Exception("Hive default schema not found after creation")
    
    print("✓ Presto Hive schema initialized")
    return True

def check_data_availability(**context):
    """Task 2: Check if POS files and stock snapshots are available"""
    execution_date = context['ds']
    
    # Check HDFS for orders
    cmd_orders = f"docker exec hadoop_client hdfs dfs -ls /procurement/raw/orders/{execution_date}"
    result = subprocess.run(cmd_orders.split(), capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Orders data not found for {execution_date}")
    
    # Check HDFS for stock
    cmd_stock = f"docker exec hadoop_client hdfs dfs -ls /procurement/raw/stock/{execution_date}"
    result = subprocess.run(cmd_stock.split(), capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Stock data not found for {execution_date}")
    
    print(f"✓ Data availability verified for {execution_date}")
    return True

def create_hive_tables(**context):
    """Task 3: Create/refresh Hive external tables"""
    execution_date = context['ds']
    
    queries = [
        "DROP TABLE IF EXISTS hive.default.orders",
        "DROP TABLE IF EXISTS hive.default.stock",
        f"""
        CREATE TABLE hive.default.orders (
            order_id VARCHAR,
            pos_store_id VARCHAR,
            sku VARCHAR,
            quantity INTEGER,
            order_date VARCHAR,
            unit_price DOUBLE
        )
        WITH (
            external_location = 'hdfs://namenode:9000/procurement/raw/orders/{execution_date}',
            format = 'JSON'
        )
        """,
        f"""
        CREATE TABLE hive.default.stock (
            warehouse_code VARCHAR,
            sku VARCHAR,
            available_stock INTEGER,
            reserved_stock INTEGER,
            snapshot_date VARCHAR
        )
        WITH (
            external_location = 'hdfs://namenode:9000/procurement/raw/stock/{execution_date}',
            format = 'JSON'
        )
        """
    ]
    
    for query in queries:
        cmd = ['docker', 'exec', 'presto', 'presto-cli', '--execute', query]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0 and "DROP TABLE" not in query:
            raise Exception(f"Failed to create tables: {result.stderr}")
    
    print("✓ Hive tables created/refreshed")
    return True

def validate_data_quality(**context):
    """Task 4: Validate data quality and detect anomalies"""
    exceptions = []
    
    # Check for missing supplier mappings
    query = """
        SELECT COUNT(DISTINCT p.sku) as unmapped_skus
        FROM postgresql.public.products p
        LEFT JOIN postgresql.public.supplier_products sp ON p.product_id = sp.product_id
        WHERE sp.supplier_id IS NULL AND p.is_active = TRUE
    """
    cmd = ['docker', 'exec', 'presto', 'presto-cli', '--execute', query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if '"0"' not in result.stdout:
        exceptions.append("WARNING: Products without supplier mappings detected")
    
    # Check for abnormal demand spikes
    query = """
        SELECT sku, SUM(quantity) as total_demand
        FROM hive.default.orders
        GROUP BY sku
        HAVING SUM(quantity) > 1000
    """
    cmd = ['docker', 'exec', 'presto', 'presto-cli', '--execute', query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if len(result.stdout.strip().split('\n')) > 1:
        exceptions.append("INFO: Abnormal demand spike detected for some SKUs")
    
    # Log exceptions
    if exceptions:
        execution_date = context['ds']
        log_path = f"/logs/exceptions/{execution_date}_exceptions.log"
        # Save exceptions (simplified - would write to HDFS in production)
        print(f"⚠ Exceptions detected:\n" + "\n".join(exceptions))
    else:
        print("✓ Data quality validation passed")
    
    return True

def calculate_net_demand(**context):
    """Task 5: Calculate net demand per SKU"""
    execution_date = context['ds']
    import tempfile
    import os
    
    # Step 1: Export aggregated orders to HDFS
    agg_query = """
        SELECT 
            sku,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT pos_store_id) as store_count,
            AVG(quantity) as avg_quantity
        FROM hive.default.orders
        GROUP BY sku
    """
    
    result = subprocess.run(
        ['docker', 'exec', 'presto', 'presto-cli', '--output-format', 'CSV', '--execute', agg_query],
        capture_output=True, text=True
    )
    
    if result.returncode == 0:
        # Save to temp file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(result.stdout)
            temp_path = f.name
        
        # Ensure HDFS directory exists
        subprocess.run(['docker', 'exec', 'hadoop_client', 'hdfs', 'dfs', '-mkdir', '-p',
                       '/procurement/processed/aggregated_orders'], capture_output=True)
        
        # Upload to HDFS
        hdfs_path = f"/procurement/processed/aggregated_orders/{execution_date}_aggregated_orders.csv"
        subprocess.run(
            ['docker', 'exec', '-i', 'hadoop_client', 'hdfs', 'dfs', '-put', '-f', '/dev/stdin', hdfs_path],
            stdin=open(temp_path, 'rb'), capture_output=True
        )
        os.unlink(temp_path)
        print(f"✓ Aggregated orders saved to HDFS: {hdfs_path}")
    
    # Step 2: Calculate net demand
    query = """
        CREATE TABLE hive.default.net_demand AS
        SELECT 
            p.sku,
            p.product_name,
            COALESCE(o.total_quantity, 0) as aggregated_orders,
            COALESCE(s.total_available, 0) as available_stock,
            COALESCE(s.total_reserved, 0) as reserved_stock,
            r.safety_stock,
            GREATEST(
                COALESCE(o.total_quantity, 0) + r.safety_stock - 
                (COALESCE(s.total_available, 0) - COALESCE(s.total_reserved, 0)),
                0
            ) as net_demand,
            r.case_size,
            r.min_order_quantity
        FROM postgresql.public.products p
        JOIN postgresql.public.replenishment_rules r ON p.product_id = r.product_id
        LEFT JOIN (
            SELECT sku, SUM(quantity) as total_quantity
            FROM hive.default.orders GROUP BY sku
        ) o ON p.sku = o.sku
        LEFT JOIN (
            SELECT sku, 
                   SUM(available_stock) as total_available,
                   SUM(reserved_stock) as total_reserved
            FROM hive.default.stock GROUP BY sku
        ) s ON p.sku = s.sku
        WHERE p.is_active = TRUE
    """
    
    # Drop existing table
    subprocess.run([
        'docker', 'exec', 'presto', 'presto-cli', '--execute',
        'DROP TABLE IF EXISTS hive.default.net_demand'
    ], capture_output=True)
    
    # Create net demand table
    cmd = ['docker', 'exec', 'presto', 'presto-cli', '--execute', query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Net demand calculation failed: {result.stderr}")
    
    # Step 3: Export net demand to HDFS
    export_query = "SELECT * FROM hive.default.net_demand"
    result = subprocess.run(
        ['docker', 'exec', 'presto', 'presto-cli', '--output-format', 'CSV', '--execute', export_query],
        capture_output=True, text=True
    )
    
    if result.returncode == 0:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(result.stdout)
            temp_path = f.name
        
        subprocess.run(['docker', 'exec', 'hadoop_client', 'hdfs', 'dfs', '-mkdir', '-p',
                       '/procurement/processed/net_demand'], capture_output=True)
        
        hdfs_path = f"/procurement/processed/net_demand/{execution_date}_net_demand.csv"
        subprocess.run(
            ['docker', 'exec', '-i', 'hadoop_client', 'hdfs', 'dfs', '-put', '-f', '/dev/stdin', hdfs_path],
            stdin=open(temp_path, 'rb'), capture_output=True
        )
        os.unlink(temp_path)
        print(f"✓ Net demand saved to HDFS: {hdfs_path}")
    
    print("✓ Net demand calculated")
    return True

def generate_supplier_orders(**context):
    """Task 6: Generate supplier orders"""
    execution_date = context['ds']
    
    query = """
        SELECT 
            s.supplier_code,
            s.supplier_name,
            nd.sku,
            nd.product_name,
            nd.net_demand,
            nd.case_size,
            CAST(CEIL(nd.net_demand * 1.0 / nd.case_size) * nd.case_size AS INTEGER) as order_quantity
        FROM hive.default.net_demand nd
        JOIN postgresql.public.products p ON nd.sku = p.sku
        JOIN postgresql.public.supplier_products sp ON p.product_id = sp.product_id
        JOIN postgresql.public.suppliers s ON sp.supplier_id = s.supplier_id
        WHERE sp.is_primary_supplier = TRUE AND nd.net_demand > 0
        ORDER BY s.supplier_code, nd.sku
    """
    
    cmd = ['docker', 'exec', 'presto', 'presto-cli', '--output-format', 'CSV', '--execute', query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Supplier order generation failed: {result.stderr}")
    
    # Save to local /data directory
    import os
    import tempfile
    output_dir = "/data/output/supplier_orders"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"supplier_orders_{execution_date.replace('-', '')}.csv"
    local_path = f"{output_dir}/{output_file}"
    with open(local_path, 'w') as f:
        f.write(result.stdout)
    
    # Also save to HDFS /output/supplier_orders/
    subprocess.run(['docker', 'exec', 'hadoop_client', 'hdfs', 'dfs', '-mkdir', '-p',
                   '/procurement/output/supplier_orders'], capture_output=True)
    
    hdfs_path = f"/procurement/output/supplier_orders/{output_file}"
    subprocess.run(
        ['docker', 'exec', '-i', 'hadoop_client', 'hdfs', 'dfs', '-put', '-f', '/dev/stdin', hdfs_path],
        stdin=open(local_path, 'rb'), capture_output=True
    )
    
    print(f"✓ Supplier orders generated: {output_file}")
    print(f"✓ Saved locally: {local_path}")
    print(f"✓ Saved to HDFS: {hdfs_path}")
    return output_file

def cleanup_temp_tables(**context):
    """Task 7: Cleanup temporary tables"""
    subprocess.run([
        'docker', 'exec', 'presto', 'presto-cli', '--execute',
        'DROP TABLE IF EXISTS hive.default.net_demand'
    ])
    print("✓ Temporary tables cleaned up")
    return True

# Define tasks
task_init_schema = PythonOperator(
    task_id='initialize_presto_schema',
    python_callable=initialize_presto_schema,
    dag=dag,
)

task_check_data = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

task_create_tables = PythonOperator(
    task_id='create_hive_tables',
    python_callable=create_hive_tables,
    dag=dag,
)

task_validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

task_calculate_demand = PythonOperator(
    task_id='calculate_net_demand',
    python_callable=calculate_net_demand,
    dag=dag,
)

task_generate_orders = PythonOperator(
    task_id='generate_supplier_orders',
    python_callable=generate_supplier_orders,
    dag=dag,
)

task_cleanup = PythonOperator(
    task_id='cleanup_temp_tables',
    python_callable=cleanup_temp_tables,
    dag=dag,
)

# Define task dependencies (pipeline flow)
task_init_schema >> task_check_data >> task_create_tables >> task_validate_quality >> task_calculate_demand >> task_generate_orders >> task_cleanup
