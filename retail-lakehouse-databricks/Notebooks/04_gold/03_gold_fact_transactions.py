# Databricks notebook source
from pyspark.sql.functions import col, when, coalesce, lit

# =========================================================
# SOURCE TABLES
# =========================================================
txn_df = spark.table("workspace.default.silver_transections")
cust_df = spark.table("workspace.default.gold_dim_customers")
prod_df = spark.table("workspace.default.gold_dim_products")

target_table = "workspace.default.gold_fact_transections"

# =========================================================
# PREPARE DIM LOOKUP TABLES
# Rename SK columns before join to avoid ambiguity
# =========================================================
cust_lkp_df = cust_df.select(
    col("customer_id"),
    col("customer_sk").alias("lkp_customer_sk")
)

prod_lkp_df = prod_df.select(
    col("product_id"),
    col("product_sk").alias("lkp_product_sk")
)

# =========================================================
# LEFT JOIN TO DIMENSIONS
# =========================================================
fact_df = (
    txn_df.alias("t")
    .join(
        cust_lkp_df.alias("c"),
        col("t.customer_id") == col("c.customer_id"),
        "left"
    )
    .join(
        prod_lkp_df.alias("p"),
        col("t.product_id") == col("p.product_id"),
        "left"
    )
)

# =========================================================
# HANDLE MISSING DIMENSIONS + BUSINESS FLAGS
# =========================================================
fact_df = (
    fact_df
    .withColumn("customer_sk", coalesce(col("lkp_customer_sk"), lit(0)))
    .withColumn("product_sk", coalesce(col("lkp_product_sk"), lit(0)))
    .withColumn(
        "is_completed_sale",
        when(col("t.status") == "COMPLETED", 1).otherwise(0)
    )
    .withColumn(
        "is_dimension_missing",
        when(
            col("lkp_customer_sk").isNull() | col("lkp_product_sk").isNull(),
            1
        ).otherwise(0)
    )
)

# =========================================================
# FINAL FACT SELECT
# =========================================================
fact_df = fact_df.select(
    col("t.transaction_id"),
    col("customer_sk"),
    col("product_sk"),
    col("t.customer_id"),
    col("t.product_id"),
    col("t.transaction_date"),
    col("t.quantity"),
    col("t.amount"),
    col("t.payment_mode"),
    col("t.status"),
    col("is_completed_sale"),
    col("is_dimension_missing"),
    col("t.transaction_year"),
    col("t.transaction_month"),
    col("t.transaction_day"),
    col("t.ingestion_timestamp"),
    col("t.source_file_name"),
    col("t.batch_id")
)

# =========================================================
# WRITE FACT TABLE
# =========================================================
fact_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

print(f"Loaded {target_table}")
print("Row count:", fact_df.count())

display(fact_df)

# COMMAND ----------

display(
    spark.table("workspace.default.gold_fact_transections")
    .groupBy("transaction_id")
    .count()
    .filter("count > 1")
)

# COMMAND ----------

from pyspark.sql.functions import col

display(
    spark.table("workspace.default.gold_fact_transections")
    .filter(col("is_dimension_missing") == 1)
)

# COMMAND ----------

display(
    spark.table("workspace.default.gold_fact_transections")
    .groupBy("status", "is_completed_sale")
    .count()
)

# COMMAND ----------

gold_transection_fact_df = spark.table("workspace.default.gold_fact_transections")
print("Row count:", gold_transection_fact_df.count())
gold_transection_fact_df.printSchema()
display(gold_transection_fact_df.orderBy("transaction_id"))
gold_transection_fact_df.show()