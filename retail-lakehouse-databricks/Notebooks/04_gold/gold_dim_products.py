# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================================
# CONFIG
# =========================================================
SOURCE_TABLE = "workspace.default.silver_products"
TARGET_TABLE = "workspace.default.gold_dim_products"

# =========================================================
# READ SOURCE
# =========================================================
src1_df = spark.table(SOURCE_TABLE)

# =========================================================
# BASIC STANDARDIZATION / COLUMN SELECTION
# Keep business key + business attributes + audit fields
# =========================================================
#base_df = (
#    src_df
 #   .select(
 #       F.col("customer_id").cast("int").alias("customer_id"),
 #       F.trim(F.col("name")).alias("customer_name"),
 #       F.upper(F.trim(F.col("country"))).alias("country"),
 #       F.col("age").cast("int").alias("age"),
 #       F.col("ingestion_timestamp"),
#        F.col("batch_id")
 ##   )
#)


base1_df = (
    src1_df
    .select(
        F.col("product_id").cast("int").alias("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.upper(F.trim(F.col("category"))).alias("category"),
        F.upper(F.trim(F.col("brand"))).alias("brand"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.col("ingestion_timestamp").alias("ingestion_timestamp"),
        F.col("source_file_name").alias("source_file_name"),
        F.col("batch_id").alias("batch_id")
    )
)


# =========================================================
# DATA QUALITY FILTERS
# =========================================================
base1_df = (
    base1_df
    .filter(F.col("product_id").isNotNull())
)

# Optional: enforce non-empty customer_name if required
# base_df = base_df.filter(F.col("customer_name").isNotNull())

# =========================================================
# DEDUPLICATION
# Keep latest record by business key
# =========================================================
dedup_window = Window.partitionBy("product_id").orderBy(F.col("ingestion_timestamp").desc())

dedup1_df = (
    base1_df
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
sk_window = Window.orderBy(F.col("product_id"))

product_dim_df = (
    dedup1_df
    .withColumn("product_sk", F.row_number().over(sk_window))
    .select(
        "product_sk",
        "product_id",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "ingestion_timestamp",
        "source_file_name",
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
            0,                  # product_sk
            -1,                 # product_id (default business key placeholder)
            "UNKNOWN",          # product_name
            "UNKNOWN",          # category
            "UNKNOWN",          # brand
            0,                 # unit_price
            None,               # ingestion_timestamp
            None,               # source_file_name
            "SYSTEM_DEFAULT"    # batch_id
        )
    ],
    schema="""
        product_sk INT,
        product_id INT,
        product_name STRING,
        category STRING,
        brand STRING,
        unit_price DOUBLE,
        ingestion_timestamp TIMESTAMP,
        source_file_name STRING,
        batch_id STRING
    """
)







# =========================================================
# FINAL DIMENSION
# =========================================================
final_dim_df = (
    unknown_df
    .unionByName(product_dim_df)
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

display(final_dim_df.orderBy("product_sk"))

# COMMAND ----------

display(
    spark.table("workspace.default.gold_dim_products")
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
)

# COMMAND ----------

display(
    spark.table("workspace.default.gold_dim_products")
    .filter(F.col("product_sk") == 0)
)

# COMMAND ----------

display(
    spark.table("workspace.default.gold_dim_products")
    .groupBy("product_sk")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
)

# COMMAND ----------

gold_product_dim_df = spark.table("workspace.default.gold_dim_products")
print("Row count:", gold_product_dim_df.count())
gold_product_dim_df.printSchema()
display(gold_product_dim_df.orderBy("product_sk"))
gold_product_dim_df.show()
