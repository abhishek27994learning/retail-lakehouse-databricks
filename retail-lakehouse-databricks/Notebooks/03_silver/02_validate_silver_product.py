# Databricks notebook source
df = spark.table("workspace.default.silver_products")

print("Row count:", df.count())
df.printSchema()
display(df)