# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col

raw_base_path = "dbfs:/Volumes/workspace/default/retail_raw_volume"

raw_customers_path = f"{raw_base_path}/customers.csv"
raw_products_path = f"{raw_base_path}/products.csv"
raw_transactions_path = f"{raw_base_path}/transactions.csv"

catalog_name = "workspace"
schema_name = "default"
batch_id = "batch_001"

full_schema = f"{catalog_name}.{schema_name}"

def load_to_bronze(csv_path: str, table_name: str):
    full_table_name = f"{full_schema}.{table_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_name", col("_metadata.file_path"))
        .withColumn("batch_id", lit(batch_id))
    )

    df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    print(f"Loaded {full_table_name}")
    display(df.limit(5))


load_to_bronze(
    raw_customers_path,
    "bronze_customers"
)

load_to_bronze(
    raw_products_path,
    "bronze_products"
)

load_to_bronze(
    raw_transactions_path,
    "bronze_transactions"
)

# COMMAND ----------

dbutils.fs.mv(
    "dbfs:/Volumes/workspace/default/retail_raw_volume/transections.csv",
    "dbfs:/Volumes/workspace/default/retail_raw_volume/transactions.csv"
)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/Volumes/workspace/default/retail_raw_volume"))