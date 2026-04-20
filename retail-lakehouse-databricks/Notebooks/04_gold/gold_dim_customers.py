# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================================
# CONFIG
# =========================================================
SOURCE_TABLE = "workspace.default.silver_customers"
TARGET_TABLE = "workspace.default.gold_dim_customers"

# =========================================================
# READ SOURCE
# =========================================================
src_df = spark.table(SOURCE_TABLE)

# =========================================================
# BASIC STANDARDIZATION / COLUMN SELECTION
# Keep business key + business attributes + audit fields
# =========================================================
base_df = (
    src_df
    .select(
        F.col("customer_id").cast("int").alias("customer_id"),
        F.trim(F.col("name")).alias("customer_name"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        F.col("age").cast("int").alias("age"),
        F.col("ingestion_timestamp"),
        F.col("batch_id")
    )
)

# =========================================================
# DATA QUALITY FILTERS
# =========================================================
base_df = (
    base_df
    .filter(F.col("customer_id").isNotNull())
)

# Optional: enforce non-empty customer_name if required
# base_df = base_df.filter(F.col("customer_name").isNotNull())

# =========================================================
# DEDUPLICATION
# Keep latest record by business key
# =========================================================
dedup_window = Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())

dedup_df = (
    base_df
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# =========================================================
# SURROGATE KEY GENERATION
# Version 1 approach: row_number ordered by business key
# Note:
# - Good for portfolio/demo
# - In large production incremental loads, SK management is more advanced
# =========================================================
sk_window = Window.orderBy(F.col("customer_id"))

customer_dim_df = (
    dedup_df
    .withColumn("customer_sk", F.row_number().over(sk_window))
    .select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "country",
        "age",
        "ingestion_timestamp",
        "batch_id"
    )
)

# =========================================================
# UNKNOWN / DEFAULT RECORD
# This protects fact table joins when customer is missing
# =========================================================
unknown_df = spark.createDataFrame(
    [
        (
            0,                  # customer_sk
            -1,                 # customer_id (default business key placeholder)
            "UNKNOWN",          # customer_name
            "UNKNOWN",          # country
            None,               # age
            None,               # ingestion_timestamp
            "SYSTEM_DEFAULT"    # batch_id
        )
    ],
    schema="""
        customer_sk INT,
        customer_id INT,
        customer_name STRING,
        country STRING,
        age INT,
        ingestion_timestamp TIMESTAMP,
        batch_id STRING
    """
)

# =========================================================
# FINAL DIMENSION
# =========================================================
final_dim_df = (
    unknown_df
    .unionByName(customer_dim_df)
)

# =========================================================
# WRITE TARGET
# Overwrite for Version 1 project build
# =========================================================
(
    final_dim_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# =========================================================
# VALIDATION / DISPLAY
# =========================================================
print(f"Loaded table: {TARGET_TABLE}")
print(f"Row count: {final_dim_df.count()}")

display(final_dim_df.orderBy("customer_sk"))

# COMMAND ----------

display(
    spark.table("workspace.default.gold_dim_customers")
    .groupBy("customer_id")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
)

# COMMAND ----------

display(
    spark.table("workspace.default.gold_dim_customers")
    .filter(F.col("customer_sk") == 0)
)

# COMMAND ----------

display(
    spark.table("workspace.default.gold_dim_customers")
    .groupBy("customer_sk")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
)

# COMMAND ----------

gold_customer_dim_df = spark.table("workspace.default.gold_dim_customers")
print("Row count:", gold_customer_dim_df.count())
gold_customer_dim_df.printSchema()
display(gold_customer_dim_df.orderBy("customer_sk"))