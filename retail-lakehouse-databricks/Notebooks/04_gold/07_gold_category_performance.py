# Databricks notebook source
from pyspark.sql import functions as F

fact_df = spark.table("workspace.default.gold_fact_transections")
prod_df = spark.table("workspace.default.gold_dim_products")

target_table = "workspace.default.gold_category_performance"

category_perf_df = (
    fact_df.alias("f")
    .filter(F.col("f.is_completed_sale") == 1)
    .join(
        prod_df.alias("p"),
        F.col("f.product_sk") == F.col("p.product_sk"),
        "left"
    )
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct("f.transaction_id").alias("total_transactions"),
        F.sum("f.quantity").alias("total_quantity"),
        F.sum("f.amount").alias("total_sales_amount"),
        F.avg("f.amount").alias("average_transaction_amount"),
        F.countDistinct("f.product_sk").alias("unique_products"),
        F.countDistinct("f.customer_sk").alias("unique_customers"),
        F.min("f.transaction_date").alias("first_sale_date"),
        F.max("f.transaction_date").alias("last_sale_date")
    )
)

(
    category_perf_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Loaded table: {target_table}")
print("Row count:", category_perf_df.count())

display(category_perf_df.orderBy(F.col("total_sales_amount").desc()))

# COMMAND ----------

category_perf = spark.table("workspace.default.gold_category_performance")
category_perf.show()