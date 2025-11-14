"""
Query the ecommerce database with JOIN operations.

This script demonstrates complex SQL queries joining multiple tables
to extract meaningful business insights.
"""

import sqlite3
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def execute_query(cursor, query, description):
    """
    Execute a query and display results in a formatted table.
    
    Args:
        cursor: SQLite cursor object
        query: SQL query string
        description: Description of the query
    """
    print("\n" + "="*100)
    print(f"QUERY: {description}")
    print("="*100)
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("No results found.")
            return
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Calculate column widths (max 25 chars per column for readability)
        col_widths = []
        for i, col in enumerate(columns):
            max_width = len(col)
            for row in rows:
                val_len = len(str(row[i]))
                max_width = max(max_width, val_len)
            # Cap width at 25 for very long fields
            col_widths.append(min(max_width, 25))
        
        # Print header with box drawing
        header_parts = []
        for i, col in enumerate(columns):
            header_parts.append(col[:col_widths[i]].ljust(col_widths[i]))
        
        border = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
        header = "| " + " | ".join(header_parts) + " |"
        
        print("\n" + border)
        print(header)
        print(border)
        
        # Print rows
        for row in rows:
            row_parts = []
            for i, val in enumerate(row):
                val_str = str(val)[:col_widths[i]].ljust(col_widths[i])
                row_parts.append(val_str)
            print("| " + " | ".join(row_parts) + " |")
        
        print(border)
        print(f"\nTotal rows: {len(rows)}\n")
        
    except sqlite3.Error as e:
        logger.error(f"Query failed: {str(e)}")


def run_queries():
    """
    Execute JOIN query on the ecommerce database.
    """
    script_dir = Path(__file__).parent
    db_path = script_dir / 'ecommerce.db'
    
    if not db_path.exists():
        logger.error(f"Database '{db_path}' not found. Run ingest_data.py first.")
        return
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logger.info(f"Connected to database: {db_path}")
        
        # Multi-table JOIN query: Complete order analysis with all related tables
        query = """
        SELECT 
            o.order_id,
            o.order_date,
            u.name AS customer_name,
            u.email,
            u.city,
            u.state,
            pr.name AS product_name,
            pr.category,
            oi.quantity,
            oi.price AS unit_price,
            (oi.quantity * oi.price) AS item_total,
            o.total_amount AS order_total,
            o.status AS order_status,
            p.payment_method,
            p.payment_date,
            p.status AS payment_status
        FROM orders o
        INNER JOIN users u ON o.user_id = u.user_id
        INNER JOIN order_items oi ON o.order_id = oi.order_id
        INNER JOIN products pr ON oi.product_id = pr.product_id
        LEFT JOIN payments p ON o.order_id = p.order_id
        ORDER BY o.order_date DESC, o.order_id, oi.order_item_id
        LIMIT 50;
        """
        execute_query(cursor, query, "Complete Order Details - Multi-Table JOIN")
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    run_queries()
