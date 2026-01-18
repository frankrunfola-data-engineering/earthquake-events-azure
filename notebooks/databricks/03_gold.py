# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold — Enrich + Aggregate
# MAGIC
# MAGIC Enrich Silver events with a country code (offline reverse geocode), classify significance, and write Gold outputs.
# MAGIC
# MAGIC Outputs:
# MAGIC - **Event-level** enriched parquet
# MAGIC - **Daily KPI rollups** (by date/country/sig_class) as CSV
# MAGIC
# MAGIC Designed to run as a Databricks Job task called **Gold**.
# COMMAND ----------
import pandas as pd
from pyspark.sql import functions as F

bronze_output = dbutils.jobs.taskValues.get(taskKey="Bronze", key="bronze_output")
silver_output = dbutils.jobs.taskValues.get(taskKey="Silver", key="silver_output")

run_date = bronze_output.get("run_date")
gold_adls = bronze_output.get("gold_adls")
silver_output_path = (
    silver_output.get("silver_output_path") if isinstance(silver_output, dict) else silver_output
)

assert run_date, "Missing run_date from bronze_output"
assert gold_adls, "Missing gold_adls from bronze_output"
assert silver_output_path, "Missing silver_output_path from silver_output"

print(f"Reading Silver from: {silver_output_path}")
df = spark.read.parquet(silver_output_path)
print(f"Silver rows: {df.count()}")
# COMMAND ----------
# Offline reverse geocoding (requires cluster library: reverse_geocoder)
try:
    import reverse_geocoder as rg
except Exception as e:
    raise RuntimeError(
        "Missing dependency: reverse_geocoder. Install it on the cluster (pip install reverse_geocoder) and re-run."
    ) from e

from pyspark.sql.functions import pandas_udf


@pandas_udf("string")
def country_code(lat: pd.Series, lon: pd.Series) -> pd.Series:
    # Vectorized country code lookup using reverse_geocoder.search(list_of_coords)
    mask = lat.notna() & lon.notna()
    out = pd.Series([None] * len(lat))
    if mask.any():
        coords = list(zip(lat[mask].astype(float), lon[mask].astype(float), strict=False))
        results = rg.search(coords)
        codes = [r.get("cc") if r else None for r in results]
        out.loc[mask] = codes
    return out


# COMMAND ----------
# Enrich + classify
df_enriched = df.withColumn(
    "country_code", country_code(F.col("latitude"), F.col("longitude"))
).withColumn(
    "sig_class",
    F.when(F.col("sig") < 100, F.lit("Low"))
    .when((F.col("sig") >= 100) & (F.col("sig") < 500), F.lit("Moderate"))
    .otherwise(F.lit("High")),
)

display(df_enriched.limit(10))
# COMMAND ----------
# Write Gold event-level parquet (idempotent by run_date)
gold_events_path = f"{gold_adls}earthquake_events_gold/run_date={run_date}/"
(df_enriched.repartition("event_date").write.mode("overwrite").parquet(gold_events_path))

event_count = df_enriched.count()
print(f"Wrote enriched events ({event_count}) -> {gold_events_path}")
# COMMAND ----------
# KPI rollups (daily x country x sig_class)
kpis = df_enriched.groupBy("event_date", "country_code", "sig_class").agg(
    F.count("*").alias("event_count"),
    F.avg("mag").alias("avg_mag"),
    F.max("mag").alias("max_mag"),
    F.expr("percentile_approx(mag, 0.95)").alias("p95_mag"),
)

gold_kpis_path = f"{gold_adls}earthquake_kpis_gold/run_date={run_date}/"
(kpis.coalesce(1).write.mode("overwrite").option("header", "true").csv(gold_kpis_path))

print(f"Wrote KPIs -> {gold_kpis_path}")
display(kpis.orderBy(F.desc("event_count")).limit(20))
# COMMAND ----------
dbutils.jobs.taskValues.set(
    key="gold_output",
    value={
        "run_date": run_date,
        "gold_events_path": gold_events_path,
        "gold_kpis_path": gold_kpis_path,
        "gold_event_count": int(event_count),
    },
)
