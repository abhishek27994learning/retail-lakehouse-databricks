# Databricks notebook source
project_name = "cloud_retail_lakehouse"

raw_base_path = f"/FileStore/{project_name}/raw"
bronze_base_path = f"/FileStore/{project_name}/bronze"
silver_base_path = f"/FileStore/{project_name}/silver"
gold_base_path = f"/FileStore/{project_name}/gold"

raw_customers_path = f"{raw_base_path}/customers.csv"
raw_products_path = f"{raw_base_path}/products.csv"
raw_transactions_path = f"{raw_base_path}/transactions.csv"

spark.sql("CREATE DATABASE IF NOT EXISTS retail_lakehouse")
spark.sql("USE retail_lakehouse")

print("Project initialized successfully")
print("Raw path:", raw_base_path)
print("Bronze path:", bronze_base_path)
print("Silver path:", silver_base_path)
print("Gold path:", gold_base_path)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/retail_raw_volume"))

# COMMAND ----------

project_name = "cloud_retail_lakehouse"

raw_base_path = "dbfs:/Volumes/workspace/default/retail_raw_volume"
bronze_base_path = "dbfs:/Volumes/workspace/default/retail_raw_volume/bronze"
silver_base_path = "dbfs:/Volumes/workspace/default/retail_raw_volume/silver"
gold_base_path = "dbfs:/Volumes/workspace/default/retail_raw_volume/gold"

raw_customers_path = f"{raw_base_path}/customers.csv"
raw_products_path = f"{raw_base_path}/products.csv"
raw_transactions_path = f"{raw_base_path}/transactions.csv"

spark.sql("CREATE DATABASE IF NOT EXISTS retail_lakehouse")
spark.sql("USE retail_lakehouse")

print("Project initialized successfully")
print("Raw path:", raw_base_path)
print("Bronze path:", bronze_base_path)
print("Silver path:", silver_base_path)
print("Gold path:", gold_base_path)
print("Customers path:", raw_customers_path)
print("Products path:", raw_products_path)
print("Transactions path:", raw_transactions_path)

# COMMAND ----------

project_name = "cloud_retail_lakehouse"

raw_base_path = "dbfs:/Volumes/workspace/default/retail_raw_volume"

raw_customers_path = f"{raw_base_path}/customers.csv"
raw_products_path = f"{raw_base_path}/products.csv"
raw_transactions_path = f"{raw_base_path}/transactions.csv"

catalog_name = "workspace"
schema_name = "default"

print("Project initialized successfully")
print("Raw path:", raw_base_path)
print("Customers path:", raw_customers_path)
print("Products path:", raw_products_path)
print("Transactions path:", raw_transactions_path)
print("Catalog:", catalog_name)
print("Schema:", schema_name)