"""Upload local data files to HDFS."""

import subprocess
import sys
from pathlib import Path
from datetime import date
import argparse

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))
from config.config import RAW_ORDERS_DIR, RAW_STOCK_DIR


def run_command(command: list[str], description: str):
    """Execute a shell command and handle errors."""
    try:
        print(f"  → {description}...")
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        if result.stdout:
            print(f"    {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"    ✗ Error: {e.stderr.strip()}")
        return False


def upload_to_hdfs(execution_date: str):
    """Upload orders and stock data to HDFS for a specific date."""
    print(f"\n{'='*60}")
    print(f"UPLOADING DATA TO HDFS - {execution_date}")
    print(f"{'='*60}\n")
    
    # Check if local data exists
    orders_dir = RAW_ORDERS_DIR / execution_date
    stock_dir = RAW_STOCK_DIR / execution_date
    
    if not orders_dir.exists():
        print(f"✗ Error: Orders directory not found: {orders_dir}")
        print(f"  Run: python scripts/data_generation/generate_orders.py")
        return False
    
    if not stock_dir.exists():
        print(f"✗ Error: Stock directory not found: {stock_dir}")
        print(f"  Run: python scripts/data_generation/generate_stock.py")
        return False
    
    # Count files to upload
    order_files = list(orders_dir.glob("*.json"))
    stock_files = list(stock_dir.glob("*.json"))
    
    print(f"Found {len(order_files)} order files and {len(stock_files)} stock files\n")
    
    # HDFS paths
    hdfs_orders_path = f"/procurement/raw/orders/{execution_date}"
    hdfs_stock_path = f"/procurement/raw/stock/{execution_date}"
    
    # Local paths (as seen from inside hadoop_client container)
    local_orders_path = f"/data/raw/orders/{execution_date}"
    local_stock_path = f"/data/raw/stock/{execution_date}"
    
    print("Step 1: Creating HDFS directories...")
    success = True
    
    # Create HDFS directories
    success &= run_command(
        ["docker", "exec", "hadoop_client", "hdfs", "dfs", "-mkdir", "-p", hdfs_orders_path],
        f"Create {hdfs_orders_path}"
    )
    
    success &= run_command(
        ["docker", "exec", "hadoop_client", "hdfs", "dfs", "-mkdir", "-p", hdfs_stock_path],
        f"Create {hdfs_stock_path}"
    )
    
    if not success:
        print("\n✗ Failed to create HDFS directories")
        return False
    
    print("\nStep 2: Uploading order files...")
    # Upload files individually to avoid wildcard issues
    for order_file in order_files:
        filename = order_file.name
        success &= run_command(
            ["docker", "exec", "hadoop_client", "hdfs", "dfs", "-put", "-f",
             f"{local_orders_path}/{filename}", f"{hdfs_orders_path}/{filename}"],
            f"Upload {filename}"
        )
    
    print("\nStep 3: Uploading stock files...")
    for stock_file in stock_files:
        filename = stock_file.name
        success &= run_command(
            ["docker", "exec", "hadoop_client", "hdfs", "dfs", "-put", "-f",
             f"{local_stock_path}/{filename}", f"{hdfs_stock_path}/{filename}"],
            f"Upload {filename}"
        )
    
    if not success:
        print("\n✗ Upload failed")
        return False
    
    print("\nStep 4: Verifying upload...")
    print(f"\n  Orders in HDFS ({hdfs_orders_path}):")
    run_command(
        ["docker", "exec", "hadoop_client", "hdfs", "dfs", "-ls", hdfs_orders_path],
        "List orders"
    )
    
    print(f"\n  Stock in HDFS ({hdfs_stock_path}):")
    run_command(
        ["docker", "exec", "hadoop_client", "hdfs", "dfs", "-ls", hdfs_stock_path],
        "List stock"
    )
    
    print(f"\n{'='*60}")
    print(f"✓ UPLOAD COMPLETE - Data available in HDFS")
    print(f"{'='*60}\n")
    print(f"Next step:")
    print(f"  python src/run_pipeline.py --date {execution_date}")
    
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Upload local data files to HDFS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload today's data
  python scripts/upload_to_hdfs.py
  
  # Upload data for specific date
  python scripts/upload_to_hdfs.py --date 2026-01-13
  
  # Upload data for yesterday
  python scripts/upload_to_hdfs.py --date yesterday
        """
    )
    
    parser.add_argument(
        "--date",
        type=str,
        default="today",
        help="Date to upload (YYYY-MM-DD, 'today', or 'yesterday'). Default: today"
    )
    
    args = parser.parse_args()
    
    # Parse date
    if args.date == "today":
        execution_date = date.today().strftime("%Y-%m-%d")
    elif args.date == "yesterday":
        from datetime import timedelta
        execution_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # Validate date format
        try:
            date.fromisoformat(args.date)
            execution_date = args.date
        except ValueError:
            print(f"✗ Error: Invalid date format '{args.date}'. Use YYYY-MM-DD")
            return 1
    
    # Upload to HDFS
    success = upload_to_hdfs(execution_date)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
