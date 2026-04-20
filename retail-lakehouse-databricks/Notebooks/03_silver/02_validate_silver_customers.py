# Databricks notebook source
df = spark.table("workspace.default.silver_customers")

print("Row count:", df.count())
df.printSchema()
display(df)

# COMMAND ----------

from pyspark.sql.functions import count, col

display(
    spark.table("workspace.default.silver_customers")
    .groupBy("customer_id")
    .agg(count("*").alias("cnt"))
    .filter(col("cnt") > 1)
)