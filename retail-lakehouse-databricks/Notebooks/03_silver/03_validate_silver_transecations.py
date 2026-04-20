# Databricks notebook source
df= spark.table("workspace.default.silver_transections")

print("row_count:" , df.count())
df.printSchema()

df.show()
display(df)


# COMMAND ----------

from pyspark.sql.functions import count, col

display(
    spark.table("workspace.default.silver_transections")
    .groupBy("transaction_id")
    .agg(count("*").alias("cnt"))
    .filter(col("cnt") > 1)
)

# COMMAND ----------

display(
    spark.table("workspace.default.silver_transections")
    .filter(
        col("transaction_id").isNull() |
        col("customer_id").isNull() |
        col("product_id").isNull()
    )
)

# COMMAND ----------

display(
    spark.table("workspace.default.silver_transections")
    .filter((col("amount") <= 0) | (col("quantity") <= 0))
)