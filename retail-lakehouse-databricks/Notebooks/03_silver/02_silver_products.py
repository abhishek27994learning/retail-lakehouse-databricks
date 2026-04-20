# Databricks notebook source
from pyspark.sql.functions import col, trim, upper

bronze_table = "workspace.default.bronze_products"
silver_table = "workspace.default.silver_products"

df = spark.table(bronze_table)

df_clean = (
    df
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("brand", upper(trim(col("brand"))))
    .withColumn("unit_price", col("unit_price").cast("double"))
)

# Remove invalid records
df_clean = df_clean.filter(col("product_id").isNotNull())
df_clean = df_clean.filter(col("unit_price") > 0)

# Deduplicate
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("product_id").orderBy(col("ingestion_timestamp").desc())

df_dedup = (
    df_clean
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

df_dedup.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"Loaded {silver_table}")
display(df_dedup)