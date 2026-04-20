# Databricks notebook source
from pyspark.sql.functions import col, trim, upper, year, month, dayofmonth

bronze_table = "workspace.default.bronze_transections"
silver_table = "workspace.default.silver_transections"

df = spark.table(bronze_table)



# df_clean for silver transections

df_clean = (


    df
    .withColumn("transaction_id", col("transaction_id").cast("int"))
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("transaction_date", col("transaction_date").cast("timestamp"))
    .withColumn("payment_mode", upper(trim(col("payment_mode"))))
    .withColumn("status", upper(trim(col("status"))))

    

)



# Remove invalid records
df_clean = df_clean.filter(col("transaction_id").isNotNull())
df_clean = df_clean.filter(col("customer_id").isNotNull())
df_clean = df_clean.filter(col("product_id").isNotNull())
df_clean = df_clean.filter(col("quantity") > 0)
df_clean = df_clean.filter(col("amount") > 0)
df_clean = df_clean.filter(col("transaction_date").isNotNull())
df_clean = df_clean.filter(col("payment_mode").isNotNull())
df_clean = df_clean.filter(col("status").isNotNull())

# Deduplicate
df_clean = df_clean.dropDuplicates(["transaction_id", "ingestion_timestamp"])


df_dedup = (

    df_clean
    .withColumn("transaction_year", year("transaction_date"))
    .withColumn("transaction_month", month("transaction_date"))
    .withColumn("transaction_day", dayofmonth("transaction_date"))
)


df_dedup.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"Loaded {silver_table}")
display(df_dedup)