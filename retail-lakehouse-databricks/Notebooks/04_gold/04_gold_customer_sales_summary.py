# Databricks notebook source
from pyspark.sql import functions as F

cust_df = spark.table("workspace.default.gold_dim_customers")
fact_df = spark.table("workspace.default.gold_fact_transections")

target_table = "workspace.default.gold_customer_sale_summary"


customer_sales_df = (
fact_df.alias("f")
.filter(F.col("is_completed_sales")==1)
.join(cust_df.alias("c"),
      F.col("f.customer_sk")==F.col("c.customer_sk"),
      "left"
      )
      .groupBy(
          F.col("f.customer_sk"),
          F.col("c.customer_id"),
          F.col("c.customer_name"),
          F.col("c.country")
      )
      .agg(
          F.countDistinct("f.transaction_id").alias("total_trnsaction"),
          F.Sum("f.quantity").alias("total_quantity"),
          F.sum("f.amount").alias("total_sales_amount"),
          F.avg("f.amount").alias("avrage_transaction_amount"),
          F.max("f.transaction_date").alias("last_transaction_date"),
          F.min("f.transaction_date").alias("first_transaction_date")
          ) 
      )
          
    (
     customer_sales_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )
      
      
      
      




print(f"Loaded table: {target_table}")
print("Row count:", customer_sales_df.count())

display(customer_sales_df.orderBy(F.col("total_sales_amount").desc()))


# COMMAND ----------

from pyspark.sql import functions as F

fact_df = spark.table("workspace.default.gold_fact_transections")
cust_df = spark.table("workspace.default.gold_dim_customers")

target_table = "workspace.default.gold_customer_sales_summary"

customer_sales_df = (
    fact_df.alias("f")
    .filter(F.col("is_completed_sale") == 1)
    .join(
        cust_df.alias("c"),
        F.col("f.customer_sk") == F.col("c.customer_sk"),
        "left"
    )
    .groupBy(
        F.col("f.customer_sk"),
        F.col("c.customer_id"),
        F.col("c.customer_name"),
        F.col("c.country")
    )
    .agg(
        F.countDistinct("f.transaction_id").alias("total_transactions"),
        F.sum("f.quantity").alias("total_quantity"),
        F.sum("f.amount").alias("total_sales_amount"),
        F.avg("f.amount").alias("average_transaction_amount"),
        F.min("f.transaction_date").alias("first_transaction_date"),
        F.max("f.transaction_date").alias("last_transaction_date")
    )
)

(
    customer_sales_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Loaded table: {target_table}")
print("Row count:", customer_sales_df.count())

display(customer_sales_df.orderBy(F.col("total_sales_amount").desc()))

# COMMAND ----------

sales_summary = spark.table("workspace.default.gold_customer_sales_summary")
sales_summary.show()

# COMMAND ----------

df = spark.table("workspace.default.gold_customer_sales_summary")
print("Row count:", df.count())
df.printSchema()
display(df.orderBy("total_sales_amount", ascending=False))

# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table("workspace.default.gold_customer_sales_summary")
    .filter(F.col("customer_sk").isNull())
)