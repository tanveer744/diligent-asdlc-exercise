"""
Load CSV files from the data folder into a SQLite database.

This script creates an ecommerce.db SQLite database and loads all CSV files
from the data directory into corresponding tables.
"""

import sqlite3
import csv
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def infer_column_type(values):
    """
    Infer SQLite data type from sample values.
    
    Args:
        values: List of sample values from the column
        
    Returns:
        str: SQLite data type
    """
    # Remove empty values
    values = [v for v in values if v]
    
    if not values:
        return 'TEXT'
    
    # Check if all values are integers
    try:
        for val in values:
            int(val)
        return 'INTEGER'
    except ValueError:
        pass
    
    # Check if all values are floats
    try:
        for val in values:
            float(val)
        return 'REAL'
    except ValueError:
        pass
    
    return 'TEXT'


def create_table_from_csv(cursor, table_name, csv_file):
    """
    Create a table based on CSV structure.
    
    Args:
        cursor: SQLite cursor object
        table_name: Name of the table to create
        csv_file: Path to CSV file
        
    Returns:
        tuple: (headers, sample_rows) for later insertion
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # Read sample rows to infer types
        sample_rows = []
        for i, row in enumerate(reader):
            sample_rows.append(row)
            if i >= 100:  # Sample first 100 rows
                break
    
    # Infer column types
    columns = []
    for i, col in enumerate(headers):
        sample_values = [row[i] for row in sample_rows if i < len(row)]
        sql_type = infer_column_type(sample_values)
        
        # Add PRIMARY KEY constraint for ID columns
        if col.endswith('_id') and col.startswith(table_name[:-1]):
            columns.append(f"{col} {sql_type} PRIMARY KEY")
        else:
            columns.append(f"{col} {sql_type}")
    
    create_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    cursor.execute(create_statement)
    logger.info(f"Created table: {table_name}")
    
    return headers


def load_csv_to_sqlite(data_dir='data', db_name='ecommerce.db'):
    """
    Load all CSV files from the data directory into SQLite database.
    
    Args:
        data_dir: Path to the directory containing CSV files
        db_name: Name of the SQLite database file
    """
    # Get the script's directory
    script_dir = Path(__file__).parent
    data_path = script_dir / data_dir
    db_path = script_dir / db_name
    
    # Check if data directory exists
    if not data_path.exists():
        logger.error(f"Data directory '{data_path}' does not exist")
        return
    
    # Get all CSV files
    csv_files = list(data_path.glob('*.csv'))
    
    if not csv_files:
        logger.warning(f"No CSV files found in '{data_path}'")
        return
    
    logger.info(f"Found {len(csv_files)} CSV file(s) to process")
    
    try:
        # Connect to SQLite database (creates if doesn't exist)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys = ON")
        
        logger.info(f"Connected to database: {db_path}")
        
        # Process each CSV file
        for csv_file in csv_files:
            table_name = csv_file.stem  # Get filename without extension
            
            try:
                logger.info(f"Processing {csv_file.name}...")
                
                # Drop table if exists (for clean reload)
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create table and get headers
                headers = create_table_from_csv(cursor, table_name, csv_file)
                
                # Read and insert data
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    
                    if not rows:
                        logger.warning(f"Skipping {csv_file.name} - file is empty")
                        continue
                    
                    logger.info(f"Loaded {len(rows)} rows from {csv_file.name}")
                    
                    # Insert data in batches
                    placeholders = ', '.join(['?' for _ in headers])
                    insert_statement = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    
                    data_to_insert = []
                    for row in rows:
                        data_to_insert.append([row[col] for col in headers])
                    
                    cursor.executemany(insert_statement, data_to_insert)
                
                # Verify insertion
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                logger.info(f"Successfully inserted {count} rows into '{table_name}'")
                
            except Exception as e:
                logger.error(f"Error processing {csv_file.name}: {str(e)}")
                conn.rollback()
                continue
        
        # Commit all changes
        conn.commit()
        logger.info("All changes committed successfully")
        
        # Print database summary
        print("\n" + "="*50)
        print("DATABASE SUMMARY")
        print("="*50)
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Table: {table_name:20s} | Rows: {count}")
        
        print("="*50)
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {str(e)}")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        
    finally:
        # Close connection
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    load_csv_to_sqlite()
