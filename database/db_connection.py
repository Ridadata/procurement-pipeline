"""Database connection and utility functions."""

import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import List, Dict, Any
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.config import DB_CONFIG


class DatabaseConnection:
    """Manages PostgreSQL database connections."""
    
    @staticmethod
    @contextmanager
    def get_connection():
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
    
    @staticmethod
    def execute_query(query: str, params: tuple = None, fetch: bool = False) -> List[Dict[str, Any]]:
        """Execute a SQL query and optionally fetch results."""
        with DatabaseConnection.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch:
                    return [dict(row) for row in cursor.fetchall()]
                return []
    
    @staticmethod
    def execute_many(query: str, data: List[tuple]):
        """Execute a query with multiple parameter sets."""
        with DatabaseConnection.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, data)
    
    @staticmethod
    def execute_script(sql_file_path: str):
        """Execute a SQL script file."""
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
        
        with DatabaseConnection.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_script)
        
        print(f"Successfully executed SQL script: {sql_file_path}")
    
    @staticmethod
    def test_connection() -> bool:
        """Test database connection."""
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1;")
                    return True
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False


def setup_database():
    """Initialize the database schema."""
    script_path = Path(__file__).parent / "schema.sql"
    
    print("Setting up database schema...")
    try:
        DatabaseConnection.execute_script(str(script_path))
        print("Database schema created successfully!")
    except Exception as e:
        print(f"Error setting up database: {e}")
        raise


if __name__ == "__main__":
    print("Testing database connection...")
    if DatabaseConnection.test_connection():
        print("✓ Database connection successful!")
        
        response = input("Do you want to set up the database schema? (yes/no): ")
        if response.lower() in ['yes', 'y']:
            setup_database()
    else:
        print("✗ Database connection failed!")
        print("Please check your database configuration in .env file")
