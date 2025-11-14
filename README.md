# Ecommerce Data Pipeline (Cursor A-SDLC Exercise)

A demonstration project showcasing end-to-end data pipeline development using AI-assisted programming with Cursor IDE.

## Overview

This project implements a complete ecommerce data pipeline through three core tasks:

1. **Synthetic Data Generation** - Created realistic ecommerce CSV datasets (users, products, orders, order_items, payments)
2. **Data Ingestion** - Built a Python script to load CSV files into a SQLite database with proper schema design
3. **SQL Analytics** - Developed complex multi-table JOIN queries to extract business insights

## Tools & Technologies

- **Cursor IDE** - AI-powered code generation and assistance
- **Python** - Data processing and database operations
- **SQLite** - Lightweight relational database
- **CSV** - Data interchange format

## Project Structure

```
diligent-ecommerce-data-pipeline/
├── data/
│   ├── users.csv           # Customer information (50 rows)
│   ├── products.csv        # Product catalog (30 rows)
│   ├── orders.csv          # Order records (100 rows)
│   ├── order_items.csv     # Line items (200 rows)
│   └── payments.csv        # Payment transactions (100 rows)
├── ingest_data.py          # Database ingestion script
├── analytics_queries.py    # SQL query demonstrations
├── ecommerce.db            # SQLite database (generated)
└── README.md
```

## How to Run

### 1. Load Data into SQLite Database

Run the ingestion script to create `ecommerce.db` and populate it with CSV data:

```powershell
python ingest_data.py
```

**Output:**
- Creates 5 tables with proper schema and data types
- Inserts all CSV rows with transaction management
- Displays summary of loaded records

### 2. Execute SQL Queries

Run analytical queries demonstrating JOIN operations:

```powershell
python analytics_queries.py
```

**Sample Queries:**
- Complete order details with customer and payment information
- Product sales performance and revenue analysis
- Customer purchase summaries and spending patterns
- Payment method preferences by geographic location
- Category performance across states

## Key Features

- **Zero Dependencies** - Uses only Python standard library (csv, sqlite3, pathlib, logging)
- **Best Practices** - Proper error handling, logging, type inference, and SQL optimization
- **Comprehensive JOINs** - Demonstrates INNER, LEFT joins with GROUP BY, HAVING, and aggregations
- **Production Ready** - Transaction management, data validation, and detailed reporting

## AI-Assisted Development

This project demonstrates effective AI prompting skills using **Cursor IDE** to:
- Generate complete Python scripts with proper structure and error handling
- Design database schemas with appropriate data types and constraints
- Create complex SQL queries following industry best practices
- Produce clean, maintainable, and well-documented code

All code, schema design, and SQL queries were generated through natural language prompts, showcasing the power of AI-assisted software development.

## Prompts Used

### Data Generation
```
Generate 5 synthetic ecommerce CSV files inside a data folder with the following row counts:
users (50), products (30), orders (100), order_items (200), payments (100).
Use realistic ecommerce-style Indian values, maintain consistent IDs across files, and follow best practices.
```

### Data Ingestion
```
Create a Python script that loads all CSV files from the data folder into a SQLite 
database named ecommerce.db. Create tables that match the CSV columns, insert all rows, 
and follow best practices.
```

### SQL Analytics
```
Write an SQL query that joins multiple tables from the ecommerce database and returns 
a combined output. Follow best practices.
```

### Documentation
```
Create a README.md for this project. Include: Project title "Ecommerce Data Pipeline 
(Cursor A-SDLC Exercise)", description of the three tasks, tools used, folder structure, 
how to run scripts, and mention this demonstrates prompting skills using Cursor.
```

---

**Project Type:** Data Engineering Exercise  
**Development Method:** AI-Assisted (Cursor IDE)  
**Status:** Complete
