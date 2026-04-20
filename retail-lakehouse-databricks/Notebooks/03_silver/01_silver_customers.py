# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    row_number
)

bronze_table = "workspace.default.bronze_customers"
silver_table = "workspace.default.silver_customers"

df = spark.table(bronze_table)

# basic cleaning
df_clean = (
    df
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("name", trim(col("name")))
    .withColumn("country", upper(trim(col("country"))))
    .withColumn("ingestion_timestamp", col("ingestion_timestamp"))
    .filter(col("customer_id").isNotNull())
    .filter(col("name").isNotNull())
    .filter(col("country").isNotNull())
)

# normalize country codes / names
df_clean = (
    df_clean
    .withColumn(
        "country",
        upper(trim(col("country")))
    )
)

# deduplicate by latest ingestion_timestamp
window_spec = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").desc())

df_dedup = (
    df_clean
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# write silver table
df_dedup.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"Loaded {silver_table}")
display(df_dedup)