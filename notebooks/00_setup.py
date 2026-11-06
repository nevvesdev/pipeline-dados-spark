# Databricks notebook source
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
print(CATALOG)

# COMMAND ----------

LAYERS = ["bronze", "silver", "gold"]

for layer in LAYERS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.nyctaxi_{layer}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE 'nyctaxi_*'"))

# COMMAND ----------

trips = spark.table("samples.nyctaxi.trips")
trips.printSchema()
print(trips.count())