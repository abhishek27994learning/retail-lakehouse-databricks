# Retail Data Lakehouse (Databricks + PySpark)

## Overview
This project demonstrates an end-to-end retail data lakehouse built using Databricks and PySpark, following the Medallion Architecture.

## Architecture
Bronze -> Silver -> Gold -> Analytics Marts

## Bronze Layer
- Raw ingestion of customer, product, and transaction data

## Silver Layer
- Data cleaning
- Standardization
- Deduplication
- Data quality filtering

## Gold Layer
### Core Star Schema
- Customer Dimension with surrogate key
- Product Dimension with surrogate key
- Transaction Fact Table
- Unknown dimension handling
- Left join strategy to preserve all transactions

### Analytics Marts
- Customer Sales Summary
- Product Sales Summary
- Daily Sales KPI
- Category Performance

## Tech Stack
- Databricks
- PySpark
- Delta Lake

## Key Concepts Used
- Medallion Architecture
- Dimensional Modeling
- Surrogate Keys
- Unknown Dimension Records
- Fact and Dimension Modeling
- Data Quality Validation

## Future Improvements
- Incremental loading with MERGE
- SCD Type 2
- Dashboard integration