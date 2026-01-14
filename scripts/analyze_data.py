"""
Data Analysis and Testing Script
Run locally to get quick overview of PostgreSQL master data and HDFS data via Presto
"""

import sys
from pathlib import Path
import subprocess
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))
from database.db_connection import DatabaseConnection


class DataAnalyzer:
    """Quick data overview analyzer"""
    
    def __init__(self):
        self.separator = "=" * 70
        
    def print_header(self, title):
        """Print formatted section header"""
        print(f"\n{self.separator}")
        print(f"  {title}")
        print(self.separator)
    
    def run_presto_query(self, query):
        """Execute Presto query and return results"""
        cmd = ['docker', 'exec', 'presto', 'presto-cli', '--output-format', 'CSV', '--execute', query]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            return None
        
        return result.stdout.strip()
    
    def run_postgres_query(self, query):
        """Execute PostgreSQL query and return results"""
        try:
            results = DatabaseConnection.execute_query(query, fetch=True)
            
            if not results:
                return None
            
            # Format as CSV
            columns = list(results[0].keys())
            output = [",".join(columns)]
            for row in results:
                output.append(",".join(str(row[col]) for col in columns))
            
            return "\n".join(output)
        except Exception as e:
            print(f"  ❌ Query failed: {e}")
            return None
    
    def format_csv_output(self, csv_data, max_rows=10):
        """Format CSV data into readable table"""
        if not csv_data:
            print("  No data")
            return
        
        lines = csv_data.strip().split('\n')
        if len(lines) == 0:
            return
        
        # Parse CSV
        rows = [line.split(',') for line in lines]
        
        # Calculate column widths
        col_widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
        
        # Print header
        header = rows[0]
        print("\n  " + " | ".join(header[i].ljust(col_widths[i]) for i in range(len(header))))
        print("  " + "-+-".join("-" * col_widths[i] for i in range(len(header))))
        
        # Print rows (limit to max_rows)
        data_rows = rows[1:max_rows+1]
        for row in data_rows:
            print("  " + " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row))))
        
        if len(rows) > max_rows + 1:
            print(f"\n  ... and {len(rows) - max_rows - 1} more rows")
    
    def analyze_master_data(self):
        """Quick overview of PostgreSQL master data"""
        self.print_header("MASTER DATA OVERVIEW (PostgreSQL)")
        
        # Simple counts
        print("\n📊 Database Summary:")
        query = """
            SELECT 
                (SELECT COUNT(*) FROM products) as products,
                (SELECT COUNT(*) FROM suppliers) as suppliers,
                (SELECT COUNT(*) FROM warehouses) as warehouses,
                (SELECT COUNT(*) FROM replenishment_rules) as rules
        """
        result = self.run_postgres_query(query)
        if result:
            self.format_csv_output(result, max_rows=1)
    
    def analyze_hdfs_data(self):
        """Quick overview of HDFS data via Presto"""
        self.print_header("HDFS DATA OVERVIEW (via Presto)")
        
        # Check available dates
        cmd = ['docker', 'exec', 'hadoop_client', 'hdfs', 'dfs', '-ls', '/procurement/raw/orders']
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print("\n  ⚠ No data in HDFS yet")
            return
        
        dates = []
        for line in result.stdout.split('\n'):
            if '/procurement/raw/orders/' in line:
                date = line.split('/')[-1].strip()
                if date and len(date) == 10:
                    dates.append(date)
        
        if not dates:
            print("\n  ⚠ No data found in HDFS")
            return
        
        latest_date = sorted(dates)[-1]
        print(f"\n📅 Latest data date: {latest_date}")
        print(f"   Available dates: {len(dates)}")
        
        # Quick orders count
        query = f"""
            CREATE TABLE IF NOT EXISTS hive.default.temp_orders (
                order_id VARCHAR,
                pos_store_id VARCHAR,
                sku VARCHAR,
                quantity INTEGER,
                order_date VARCHAR,
                unit_price DOUBLE
            )
            WITH (
                external_location = 'hdfs://namenode:9000/procurement/raw/orders/{latest_date}',
                format = 'JSON'
            )
        """
        self.run_presto_query(query)
        
        query = """
            SELECT 
                COUNT(*) as order_items,
                COUNT(DISTINCT sku) as unique_products,
                SUM(quantity) as total_quantity
            FROM hive.default.temp_orders
        """
        print("\n📦 Orders Summary:")
        result = self.run_presto_query(query)
        if result:
            self.format_csv_output(result, max_rows=1)
        
        # Quick stock count
        query = f"""
            CREATE TABLE IF NOT EXISTS hive.default.temp_stock (
                warehouse_code VARCHAR,
                sku VARCHAR,
                available_stock INTEGER,
                reserved_stock INTEGER,
                snapshot_date VARCHAR
            )
            WITH (
                external_location = 'hdfs://namenode:9000/procurement/raw/stock/{latest_date}',
                format = 'JSON'
            )
        """
        self.run_presto_query(query)
        
        query = """
            SELECT 
                COUNT(*) as stock_records,
                SUM(available_stock) as total_available,
                SUM(reserved_stock) as total_reserved
            FROM hive.default.temp_stock
        """
        print("\n📊 Stock Summary:")
        result = self.run_presto_query(query)
        if result:
            self.format_csv_output(result, max_rows=1)
        
        # Return latest date for combined analysis
        return latest_date
    
    def analyze_combined_data(self, latest_date):
        """Combine HDFS orders with PostgreSQL product master data"""
        self.print_header("COMBINED ANALYSIS (HDFS + PostgreSQL)")
        
        print("\n🔗 Top products ordered from HDFS...")
        
        # Get top ordered products from HDFS
        query = """
            SELECT 
                sku,
                SUM(quantity) as total_ordered,
                COUNT(DISTINCT pos_store_id) as stores
            FROM hive.default.temp_orders
            GROUP BY sku
            ORDER BY total_ordered DESC
            LIMIT 10
        """
        result = self.run_presto_query(query)
        
        if result:
            self.format_csv_output(result, max_rows=10)
        
        print("\n🔗 Products with their suppliers from PostgreSQL...")
        
        # Get products with suppliers from PostgreSQL
        query = """
            SELECT 
                p.sku,
                p.product_name,
                p.category,
                s.supplier_code
            FROM products p
            LEFT JOIN supplier_products sp ON p.product_id = sp.product_id AND sp.is_primary_supplier = TRUE
            LEFT JOIN suppliers s ON sp.supplier_id = s.supplier_id
            LIMIT 10
        """
        result = self.run_postgres_query(query)
        
        if result:
            self.format_csv_output(result, max_rows=10)
    
    def run_analysis(self):
        """Run quick data analysis"""
        print("\n" + "=" * 70)
        print("  PROCUREMENT PIPELINE - DATA OVERVIEW")
        print("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 70)
        
        self.analyze_master_data()
        latest_date = self.analyze_hdfs_data()
        
        # Run combined analysis if we have HDFS data
        if latest_date:
            self.analyze_combined_data(latest_date)
        
        print("\n" + "=" * 70)
        print("  ✅ Analysis complete!")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    analyzer = DataAnalyzer()
    analyzer.run_analysis()

