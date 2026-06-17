# Databricks notebook source
# DBTITLE 1,AMPCORE — Create gold view + seed connection_config
"""
Idempotent setup task. Run once per environment after the AMPCORE LDP pipeline has
produced at least one day of gold rows. Re-run whenever sensors are added.

1. Creates (or replaces) a regular UC view views.ampcore_15min that aggregates
   f_ampcore_econnection_measurements (30-second resolution) to 15-minute buckets
   so b10's completeness math (60 // expected_interval_minutes = 4 records/hour)
   is honest.

2. Upserts one connection_config row per live EConnection metering point,
   pointing gold_table_name at the view. filter_clause is built in Python so
   quoting is always correct.
"""

# COMMAND ----------

import json
from datetime import datetime, timezone
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType,
)

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
if not CATALOG_NAME:
    raise ValueError("catalog_name widget must be set (e.g. acme_dev or acme_prod)")

# COMMAND ----------

# MAGIC %run ../../../shared/connection_status_utils

# COMMAND ----------

# DBTITLE 1,Ensure metadata tables exist
# connection_config must exist before the MERGE below. Shared DDL — single
# source of truth in ensure_metadata_tables() (%run'd above).
ensure_metadata_tables(spark, CATALOG_NAME)

# COMMAND ----------

# DBTITLE 1,Create UC view views.ampcore_15min
# Regular view — no data stored, zero storage cost.
# The 2-day lookback is evaluated at query time; b10 adds its own date filter on top.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_NAME}.views.ampcore_15min AS
SELECT
    sk_econnection,
    gateway_id,
    object_id,
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(timestamp) / 900) * 900) AS timestamp_bucket,
    ROUND(AVG(current_rms), 2)          AS current_rms,
    ROUND(AVG(current), 2)              AS current,
    ROUND(AVG(dc_current), 2)           AS dc_current,
    ROUND(AVG(power), 2)                AS power,
    MAX(energy_cumulative)              AS energy_cumulative,
    ROUND(SUM(energy_interval), 2)      AS energy_interval,
    MAX(gold_processing_timestamp)      AS gold_processing_timestamp
FROM {CATALOG_NAME}.gold.f_ampcore_econnection_measurements
WHERE timestamp >= DATEADD(DAY, -2, CURRENT_TIMESTAMP())
GROUP BY
    sk_econnection,
    gateway_id,
    object_id,
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(timestamp) / 900) * 900)
""")
print(f"✓ {CATALOG_NAME}.views.ampcore_15min created/replaced")

# COMMAND ----------

# DBTITLE 1,Read latest EConnection inventory from d_ampcore_econnections
econnections_df = spark.sql(f"""
    SELECT sk_econnection, gateway_id, object_id, name
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY gateway_id, object_id
                   ORDER BY snapshot_date DESC
               ) AS rn
        FROM {CATALOG_NAME}.gold.d_ampcore_econnections
    )
    WHERE rn = 1
""")

econnections = econnections_df.collect()
print(f"Found {len(econnections)} EConnection(s) in {CATALOG_NAME}.gold.d_ampcore_econnections")

# COMMAND ----------

# DBTITLE 1,Build connection_config rows
now = datetime.now(timezone.utc)
config_rows = []

for e in econnections:
    sk_econnection = e["sk_econnection"]
    gateway_id     = e["gateway_id"]
    object_id      = str(e["object_id"])
    name           = e["name"]

    # Python f-string guarantees correct quoting — no CHAR(39) tricks needed.
    filter_clause = f"sk_econnection = '{sk_econnection}'"

    other_metadata = json.dumps({
        # Keys consumed by b10 (status snapshot against the 15-min view)
        "sk_econnection": sk_econnection,
        "gateway_id":     gateway_id,
        "object_id":      object_id,
        "timestamp_col":   "timestamp_bucket",
        "filter_clause":   filter_clause,
    })

    config_rows.append({
        "connection_id":            f"ampcore_{gateway_id}_{object_id}",
        "data_name":                name,
        "type":                    "IoT Sensor",
        "data_type":                "Current Sensor",
        "protocol":                "HTTP",
        "location":                None,
        "object_id":                gateway_id,
        "client":                  "ACME",
        "brand":                   "AMPCORE",
        "connector":               "ampcore_api",
        "processing_type":          "Batch",
        "job_id":                   None,
        "job_name":                 "acme ampcore batch job",
        "gold_table_name":           f"{CATALOG_NAME}.views.ampcore_15min",
        "expected_interval_minutes": 15,
        "aggregation_level":        "quarterly",
        "delayed_threshold_days":    1,
        "offline_threshold_days":    4,
        "other_metadata":           other_metadata,
        "created_at":               now,
        "updated_at":               now,
    })

# COMMAND ----------

# DBTITLE 1,Upsert connection_config (idempotent MERGE)
_schema = StructType([
    StructField("connection_id",            StringType(),    False),
    StructField("data_name",                StringType(),    True),
    StructField("type",                    StringType(),    True),
    StructField("data_type",                StringType(),    True),
    StructField("protocol",                StringType(),    True),
    StructField("location",                StringType(),    True),
    StructField("object_id",                StringType(),    True),
    StructField("client",                  StringType(),    True),
    StructField("brand",                   StringType(),    True),
    StructField("connector",               StringType(),    True),
    StructField("processing_type",          StringType(),    True),
    StructField("job_id",                   StringType(),    True),
    StructField("job_name",                 StringType(),    True),
    StructField("gold_table_name",           StringType(),    True),
    StructField("expected_interval_minutes", IntegerType(),   True),
    StructField("aggregation_level",        StringType(),    True),
    StructField("delayed_threshold_days",    IntegerType(),   True),
    StructField("offline_threshold_days",    IntegerType(),   True),
    StructField("other_metadata",           StringType(),    True),
    StructField("created_at",               TimestampType(), True),
    StructField("updated_at",               TimestampType(), True),
])

_source_df = spark.createDataFrame(config_rows, schema=_schema)
_source_df.createOrReplaceTempView("_ampcore_config_source")

spark.sql(f"""
MERGE INTO {CATALOG_NAME}.metadata.connection_config AS target
USING _ampcore_config_source AS source
ON target.connection_id = source.connection_id
WHEN MATCHED THEN UPDATE SET
    data_name                = source.data_name,
    type                    = source.type,
    data_type                = source.data_type,
    protocol                = source.protocol,
    location                = source.location,
    object_id                = source.object_id,
    client                  = source.client,
    brand                   = source.brand,
    connector               = source.connector,
    processing_type          = source.processing_type,
    job_id                   = source.job_id,
    job_name                 = source.job_name,
    gold_table_name           = source.gold_table_name,
    expected_interval_minutes = source.expected_interval_minutes,
    aggregation_level        = source.aggregation_level,
    delayed_threshold_days    = source.delayed_threshold_days,
    offline_threshold_days    = source.offline_threshold_days,
    other_metadata           = source.other_metadata,
    updated_at               = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    connection_id, data_name, type, data_type, protocol, location, object_id,
    client, brand, connector, processing_type, job_id, job_name, gold_table_name,
    expected_interval_minutes, aggregation_level, delayed_threshold_days, offline_threshold_days,
    other_metadata, created_at, updated_at
) VALUES (
    source.connection_id, source.data_name, source.type, source.data_type, source.protocol,
    source.location, source.object_id, source.client, source.brand, source.connector,
    source.processing_type, source.job_id, source.job_name, source.gold_table_name,
    source.expected_interval_minutes, source.aggregation_level, source.delayed_threshold_days,
    source.offline_threshold_days, source.other_metadata, source.created_at, source.updated_at
)
""")

_row_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {CATALOG_NAME}.metadata.connection_config
    WHERE connector = 'ampcore_api'
""").collect()[0]["cnt"]
print(f"✓ {CATALOG_NAME}.metadata.connection_config — {_row_count} ampcore_api row(s) present")
