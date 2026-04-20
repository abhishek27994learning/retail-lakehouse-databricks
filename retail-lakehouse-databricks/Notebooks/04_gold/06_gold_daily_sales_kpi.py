# Databricks notebook source
from pyspark.sql import functions as F

fact_df = spark.table("workspace.default.gold_fact_transections")

target_table = "workspace.default.gold_daily_sales_kpi"

daily_sales_df = (
    fact_df
    .filter(F.col("is_completed_sale") == 1)
    .groupBy(F.col("transaction_date"))
    .agg(
        F.countDistinct("transaction_id").alias("total_transactions"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("amount").alias("total_sales_amount"),
        F.countDistinct("customer_sk").alias("unique_customers"),
        F.countDistinct("product_sk").alias("unique_products"),
        F.avg("amount").alias("average_transaction_amount")
    )
)

(
    daily_sales_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Loaded table: {target_table}")
print("Row count:", daily_sales_df.count())

display(daily_sales_df.orderBy("transaction_date"))

# COMMAND ----------

daily_sales = spark.table("workspace.default.gold_daily_sales_kpi")
daily_sales.show()