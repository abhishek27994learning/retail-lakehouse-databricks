# Data Lakehouse Architecture (Retail Project)

## Overview
This project implements an end-to-end Data Lakehouse using Databricks and PySpark, following Medallion Architecture and Star Schema modeling.

---

## Architecture Flow

Raw CSV Data
↓
Bronze Layer (Raw Ingestion)
↓
Silver Layer (Cleaned & Standardized Data)
↓
Gold Layer (Star Schema)
↓
Analytics Marts


---

## Bronze Layer
- Ingest raw data from CSV files
- No transformations applied
- Maintains source-level fidelity

Tables:
- bronze_customers
- bronze_products
- bronze_transactions

---

## Silver Layer
- Data cleaning and standardization
- Deduplication using window functions
- Type casting and null filtering

Tables:
- silver_customers
- silver_products
- silver_transactions

---

## Gold Layer (Star Schema)

### Dimensions
- gold_dim_customers
  - surrogate key: customer_sk
  - business key: customer_id
  - includes UNKNOWN record (customer_sk = 0)

- gold_dim_products
  - surrogate key: product_sk
  - business key: product_id
  - includes UNKNOWN record (product_sk = 0)

### Fact Table
- gold_fact_transactions
  - grain: one row per transaction
  - uses LEFT JOIN with dimensions
  - default surrogate keys (0) for missing dimensions
  - flags:
    - is_completed_sale
    - is_dimension_missing

---

## Analytics Marts

### Customer Sales Summary
- total sales per customer
- first and last transaction

### Product Sales Summary
- product-level revenue and quantity

### Daily Sales KPI
- daily transactions and revenue

### Category Performance
- category-level analysis

---

## Key Concepts Used

- Medallion Architecture
- Star Schema (Fact & Dimension Modeling)
- Surrogate Keys
- Unknown Dimension Handling
- Data Quality Checks
- Window Functions (ROW_NUMBER)
- PySpark Transformations

---

## Future Enhancements

- SCD Type 2 for dimensions
- Incremental processing (MERGE)
- Dashboard layer (Power BI / Tableau)