# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SILVER = f"{CATALOG}.nyctaxi_silver.trips"
GOLD = f"{CATALOG}.nyctaxi_gold"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.faturamento_por_hora AS
SELECT
    pickup_hour,
    COUNT(*) AS total_corridas,
    ROUND(SUM(fare_amount), 2) AS faturamento_total,
    ROUND(AVG(fare_amount), 2) AS ticket_medio,
    ROUND(AVG(trip_distance), 2) AS distancia_media,
    ROUND(AVG(trip_duration_min), 2) AS duracao_media_min,
    ROUND(100 * SUM(fare_amount) / SUM(SUM(fare_amount)) OVER (), 2) AS pct_faturamento,
    RANK() OVER (ORDER BY SUM(fare_amount) DESC) AS ranking
FROM {SILVER}
GROUP BY pickup_hour
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.faturamento_por_cep AS
SELECT
    pickup_zip,
    COUNT(*) AS total_corridas,
    ROUND(SUM(fare_amount), 2) AS faturamento_total,
    ROUND(AVG(fare_amount), 2) AS ticket_medio,
    ROUND(AVG(trip_distance), 2) AS distancia_media,
    ROUND(100 * SUM(fare_amount) / SUM(SUM(fare_amount)) OVER (), 2) AS pct_faturamento,
    RANK() OVER (ORDER BY SUM(fare_amount) DESC) AS ranking
FROM {SILVER}
GROUP BY pickup_zip
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.faturamento_hora_cep AS
SELECT
    pickup_hour,
    pickup_zip,
    COUNT(*) AS total_corridas,
    ROUND(SUM(fare_amount), 2) AS faturamento_total,
    ROUND(AVG(fare_amount), 2) AS ticket_medio,
    RANK() OVER (ORDER BY SUM(fare_amount) DESC) AS ranking_geral,
    RANK() OVER (PARTITION BY pickup_hour ORDER BY SUM(fare_amount) DESC) AS ranking_na_hora
FROM {SILVER}
GROUP BY pickup_hour, pickup_zip
""")

# COMMAND ----------

silver_df = spark.table(SILVER)
silver_corridas = silver_df.count()
silver_faturamento = silver_df.agg(F.sum("fare_amount")).first()[0]

for table in ["faturamento_por_hora", "faturamento_por_cep", "faturamento_hora_cep"]:
    totals = spark.table(f"{GOLD}.{table}").agg(
        F.sum("total_corridas").alias("corridas"),
        F.sum("faturamento_total").alias("faturamento"),
    ).first()
    corridas_ok = totals["corridas"] == silver_corridas
    faturamento_ok = abs(totals["faturamento"] - silver_faturamento) < 0.5
    print(table, corridas_ok, faturamento_ok)

# COMMAND ----------

display(spark.table(f"{GOLD}.faturamento_por_hora").orderBy("pickup_hour"))

# COMMAND ----------

display(spark.table(f"{GOLD}.faturamento_por_cep").orderBy("ranking").limit(10))

# COMMAND ----------

display(
    spark.table(f"{GOLD}.faturamento_hora_cep")
    .filter("total_corridas >= 10")
    .orderBy("ranking_geral")
    .limit(10)
)

# COMMAND ----------

display(spark.table(f"{GOLD}.faturamento_por_hora").orderBy("ranking").limit(5))

# COMMAND ----------

display(
    spark.table(f"{GOLD}.faturamento_hora_cep")
    .filter("ranking_na_hora = 1")
    .orderBy("pickup_hour")
    .select("pickup_hour", "pickup_zip", "total_corridas", "faturamento_total", "ticket_medio")
)