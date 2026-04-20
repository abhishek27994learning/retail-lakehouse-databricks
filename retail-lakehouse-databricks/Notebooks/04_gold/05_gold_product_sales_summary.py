# Databricks notebook source
from pyspark.sql import functions as F

fact_df = spark.table("workspace.default.gold_fact_transections")
prod_df = spark.table("workspace.default.gold_dim_products")

target_table = "workspace.default.gold_product_sales_summary"

product_sales_df = (
    fact_df.alias("f")
    .filter(F.col("is_completed_sale") == 1)
    .join(
        prod_df.alias("p"),
        F.col("f.product_sk") == F.col("p.product_sk"),
        "left"
    )
    .groupBy(
        F.col("f.product_sk"),
        F.col("p.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.brand")
    )
    .agg(
        F.countDistinct("f.transaction_id").alias("total_transactions"),
        F.sum("f.quantity").alias("total_quantity_sold"),
        F.sum("f.amount").alias("total_sales_amount"),
        F.avg("f.amount").alias("average_transaction_amount"),
        F.min("f.transaction_date").alias("first_sale_date"),
        F.max("f.transaction_date").alias("last_sale_date")
    )
)

(
    product_sales_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Loaded table: {target_table}")
print("Row count:", product_sales_df.count())

display(product_sales_df.orderBy(F.col("total_sales_amount").desc()))

# COMMAND ----------

product_sales = spark.table("workspace.default.gold_product_sales_summary")
product_sales.show()