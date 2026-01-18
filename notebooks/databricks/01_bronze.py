# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — USGS Earthquake API → Raw JSON
# MAGIC
# MAGIC Fetch GeoJSON from the USGS API for a date range (typically daily), then write raw data to ADLS Gen2 (Bronze).
# MAGIC
# MAGIC This notebook is designed to run as a Databricks Job task called **Bronze** and pass paths/params to downstream tasks via `dbutils.jobs.taskValues`.
# COMMAND ----------
# Databricks widgets (ADF / Jobs can override these)
dbutils.widgets.text("storage_account", "rtyjkjhjytre456y")
dbutils.widgets.text("bronze_container", "bronze")
dbutils.widgets.text("silver_container", "silver")
dbutils.widgets.text("gold_container", "gold")
dbutils.widgets.text("start_date", "")  # YYYY-MM-DD (optional)
dbutils.widgets.text("end_date", "")  # YYYY-MM-DD (optional)
dbutils.widgets.text("lookback_days", "1")
dbutils.widgets.text("min_magnitude", "")  # optional

storage_account = dbutils.widgets.get("storage_account").strip()
bronze_container = dbutils.widgets.get("bronze_container").strip()
silver_container = dbutils.widgets.get("silver_container").strip()
gold_container = dbutils.widgets.get("gold_container").strip()
start_date_str = dbutils.widgets.get("start_date").strip()
end_date_str = dbutils.widgets.get("end_date").strip()
lookback_days = int(dbutils.widgets.get("lookback_days").strip() or "1")
min_magnitude_str = dbutils.widgets.get("min_magnitude").strip()

assert storage_account, "storage_account is required"

bronze_adls = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/"
silver_adls = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/"
gold_adls = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/"

# Optional: quick access check (won't fail the run if listing is blocked)
try:
    _ = dbutils.fs.ls(bronze_adls)
except Exception as e:
    print(f"[WARN] Could not list {bronze_adls}: {e}")
# COMMAND ----------
import json
from datetime import date, datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _parse_yyyy_mm_dd(s: str):
    return datetime.strptime(s, "%Y-%m-%d").date()


# Date range
if start_date_str:
    start_date = _parse_yyyy_mm_dd(start_date_str)
else:
    start_date = date.today() - timedelta(days=lookback_days)

if end_date_str:
    end_date = _parse_yyyy_mm_dd(end_date_str)
else:
    end_date = date.today()

if end_date < start_date:
    raise ValueError(f"end_date ({end_date}) must be >= start_date ({start_date})")

run_date = start_date.isoformat()  # used as the partition folder in ADLS
print(f"Pulling USGS earthquakes from {start_date} to {end_date} (run_date={run_date})")

# Requests session w/ retries
session = requests.Session()
retries = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
session.mount("https://", HTTPAdapter(max_retries=retries))

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
params = {
    "format": "geojson",
    "starttime": start_date.isoformat(),
    "endtime": end_date.isoformat(),
}
if min_magnitude_str:
    params["minmagnitude"] = float(min_magnitude_str)

resp = session.get(USGS_URL, params=params, timeout=60)
resp.raise_for_status()
geojson = resp.json()
features = geojson.get("features", [])
print(f"USGS returned {len(features)} events")

if not features:
    # Don't hard-fail. Just pass empty outputs downstream so Silver/Gold can no-op if desired.
    print("[INFO] No events returned for the date range.")
# COMMAND ----------
# Write raw data to Bronze
# NOTE: We write TWO things:
#  1) Full payload (single JSON blob) for reprocessing/audit
#  2) Features-only (JSON objects) for easy Spark reads

bronze_payload_path = f"{bronze_adls}earthquake_payload/run_date={run_date}/"
bronze_features_path = f"{bronze_adls}earthquake_features/run_date={run_date}/"
legacy_features_path = f"{bronze_adls}{run_date}_earthquake_data.json"  # kept for backwards-compat

# 1) Full payload as text (directory w/ a single part file)
payload_rdd = spark.sparkContext.parallelize([json.dumps(geojson)])
payload_rdd.coalesce(1).saveAsTextFile(bronze_payload_path)

# 2) Features as JSON objects (directory)
feature_json = [json.dumps(f) for f in features]
features_rdd = spark.sparkContext.parallelize(feature_json)
features_df = spark.read.json(features_rdd)
features_df.write.mode("overwrite").json(bronze_features_path)

# Legacy single-path read location (still a directory, Spark can read it)
features_df.write.mode("overwrite").json(legacy_features_path)

print(f"Wrote payload  -> {bronze_payload_path}")
print(f"Wrote features -> {bronze_features_path}")
print(f"Wrote legacy   -> {legacy_features_path}")
# COMMAND ----------
# Pass outputs to downstream tasks
output_data = {
    "run_date": run_date,
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "storage_account": storage_account,
    "bronze_adls": bronze_adls,
    "silver_adls": silver_adls,
    "gold_adls": gold_adls,
    "bronze_payload_path": bronze_payload_path,
    "bronze_features_path": bronze_features_path,
    "legacy_features_path": legacy_features_path,
    "event_count": int(len(features)),
}
dbutils.jobs.taskValues.set(key="bronze_output", value=output_data)
print("bronze_output:", output_data)
