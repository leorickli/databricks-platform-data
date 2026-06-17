# Databricks notebook source
# DBTITLE 1,Smartnode — Create gold views + seed connection_config
"""
Idempotent setup task. Run once per environment after acme_smartnode_pipeline has
produced at least one day of gold rows. Re-run whenever energyassets are added.

A Smartnode energyasset corresponds to a Building containing an EConnection and
(optionally) a PVInstallation. normalized gold splits the single API entity
into two facts and two dims, so this task emits TWO connection_config rows per
energyasset — one for the EConnection (grid import/export) and one for the
PVInstallation (PV production). The PV connection's data will simply stay empty
for assets that don't have PV; b10 handles empty raw-gold gracefully.

1. Creates (or replaces) views.smartnode_econnection_hourly and
   views.smartnode_pvinstallation_hourly — regular UC views with a 2-day lookback
   so b10's completeness math (60 // 60 = 1 record/hour) is honest.

2. Upserts one connection_config row per (accountId, energyassetId, asset class)
   read from gold.d_smartnode_econnections / gold.d_smartnode_pvinstallations.
"""

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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

# DBTITLE 1,Create per-asset-class hourly views
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_NAME}.views.smartnode_econnection_hourly AS
SELECT
    sk_econnection,
    account_id,
    energyasset_id,
    timestamp,
    event_date,
    energy_import,
    energy_export,
    gold_processing_timestamp
FROM {CATALOG_NAME}.gold.f_smartnode_econnection_measurements
WHERE timestamp >= DATEADD(DAY, -2, CURRENT_TIMESTAMP())
""")
print(f"✓ {CATALOG_NAME}.views.smartnode_econnection_hourly created/replaced")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_NAME}.views.smartnode_pvinstallation_hourly AS
SELECT
    sk_pvinstallation,
    account_id,
    energyasset_id,
    timestamp,
    event_date,
    energy_production,
    gold_processing_timestamp
FROM {CATALOG_NAME}.gold.f_smartnode_pvinstallation_measurements
WHERE timestamp >= DATEADD(DAY, -2, CURRENT_TIMESTAMP())
""")
print(f"✓ {CATALOG_NAME}.views.smartnode_pvinstallation_hourly created/replaced")

# COMMAND ----------

# DBTITLE 1,Read latest inventory from both dims
econnections_df = spark.sql(f"""
    SELECT sk_econnection, account_id, energyasset_id
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY account_id, energyasset_id
                   ORDER BY snapshot_date DESC
               ) AS rn
        FROM {CATALOG_NAME}.gold.d_smartnode_econnections
    )
    WHERE rn = 1
""")

pvinstallations_df = spark.sql(f"""
    SELECT sk_pvinstallation, account_id, energyasset_id
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY account_id, energyasset_id
                   ORDER BY snapshot_date DESC
               ) AS rn
        FROM {CATALOG_NAME}.gold.d_smartnode_pvinstallations
    )
    WHERE rn = 1
""")

econnections    = econnections_df.collect()
pvinstallations = pvinstallations_df.collect()
print(f"Found {len(econnections)} EConnection(s) and {len(pvinstallations)} PVInstallation(s)")

# COMMAND ----------

# DBTITLE 1,Build connection_config rows (one per asset)
now = datetime.now(timezone.utc)
config_rows = []

for e in econnections:
    sk_econnection = e["sk_econnection"]
    account_id     = str(e["account_id"])
    energyasset_id = str(e["energyasset_id"])

    filter_clause = f"sk_econnection = '{sk_econnection}'"

    other_metadata = json.dumps({
        "sk_econnection": sk_econnection,
        "account_id":     account_id,
        "energyasset_id": energyasset_id,
        "timestamp_col":   "timestamp",
        "filter_clause":   filter_clause,
    })

    config_rows.append({
        "connection_id":            f"smartnode_econn_{account_id}_{energyasset_id}",
        "data_name":                f"Smartnode EConnection {energyasset_id}",
        "type":                    "Energy Asset",
        "data_type":                "Electricity Meter",
        "protocol":                "HTTP",
        "location":                None,
        "object_id":                energyasset_id,
        "client":                  "ACME",
        "brand":                   "Smartnode",
        "connector":               "smartnode_api",
        "processing_type":          "Batch",
        "job_id":                   None,
        "job_name":                 "acme smartnode batch job",
        "gold_table_name":           f"{CATALOG_NAME}.views.smartnode_econnection_hourly",
        "expected_interval_minutes": 60,
        "aggregation_level":        "hourly",
        "delayed_threshold_days":    1,
        "offline_threshold_days":    4,
        "other_metadata":           other_metadata,
        "created_at":               now,
        "updated_at":               now,
    })

for p in pvinstallations:
    sk_pv          = p["sk_pvinstallation"]
    account_id     = str(p["account_id"])
    energyasset_id = str(p["energyasset_id"])

    filter_clause = f"sk_pvinstallation = '{sk_pv}'"

    other_metadata = json.dumps({
        "sk_pvinstallation": sk_pv,
        "account_id":        account_id,
        "energyasset_id":    energyasset_id,
        "timestamp_col":      "timestamp",
        "filter_clause":      filter_clause,
    })

    config_rows.append({
        "connection_id":            f"smartnode_pv_{account_id}_{energyasset_id}",
        "data_name":                f"Smartnode PVInstallation {energyasset_id}",
        "type":                    "Energy Asset",
        "data_type":                "PV Installation",
        "protocol":                "HTTP",
        "location":                None,
        "object_id":                energyasset_id,
        "client":                  "ACME",
        "brand":                   "Smartnode",
        "connector":               "smartnode_api",
        "processing_type":          "Batch",
        "job_id":                   None,
        "job_name":                 "acme smartnode batch job",
        "gold_table_name":           f"{CATALOG_NAME}.views.smartnode_pvinstallation_hourly",
        "expected_interval_minutes": 60,
        "aggregation_level":        "hourly",
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
_source_df.createOrReplaceTempView("_smartnode_config_source")

spark.sql(f"""
MERGE INTO {CATALOG_NAME}.metadata.connection_config AS target
USING _smartnode_config_source AS source
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
    WHERE connector = 'smartnode_api'
""").collect()[0]["cnt"]
print(f"✓ {CATALOG_NAME}.metadata.connection_config — {_row_count} smartnode_api row(s) present")
