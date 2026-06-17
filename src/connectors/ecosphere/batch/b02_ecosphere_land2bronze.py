# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, ArrayType
)

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "ecosphere_batch", "Volume Name")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}/"

# Bronze table paths
BRONZE_HISTORY        = f"{CATALOG_NAME}.bronze.ecosphere_batch_history"
BRONZE_METERREADINGS  = f"{CATALOG_NAME}.bronze.ecosphere_batch_meterreadings"
BRONZE_POINT_VALUE    = f"{CATALOG_NAME}.bronze.ecosphere_batch_point_value"
BRONZE_EV_SOCKET      = f"{CATALOG_NAME}.bronze.ecosphere_batch_ev_socket"

# One checkpoint per bronze table
CHECKPOINT_HISTORY       = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/ecosphere_batch_history/"
CHECKPOINT_METERREADINGS = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/ecosphere_batch_meterreadings/"
CHECKPOINT_POINT_VALUE   = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/ecosphere_batch_point_value/"
CHECKPOINT_EV_SOCKET     = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/ecosphere_batch_ev_socket/"

# Endpoint routing — which endpoint_name belongs to which bronze group
HISTORY_ENDPOINTS       = ["electricity_history_day", "heat_history_day", "water_history_day", "gas_history_day", "fcr_history_quarter"]
METERREADINGS_ENDPOINTS = ["electricity_meterreadings", "heat_meterreadings", "water_meterreadings"]
POINT_VALUE_ENDPOINTS   = ["electricity_soc", "solar_irradiance"]
EV_SOCKET_ENDPOINTS     = ["ev_socket"]

# COMMAND ----------

# DBTITLE 1,Create Bronze Tables

# --- bronze.ecosphere_history ---
# One row per 15-min interval per building per energy type.
# Populated from: electricity_history_day, heat_history_day, water_history_day, gas_history_day, fcr_history_quarter.
# Source shape: {start_datetime, end_datetime, values: [{datetime, value}, ...]}
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_HISTORY} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    endpoint_name              STRING    COMMENT 'e.g., electricity_history_day, gas_history_day',
    energy_method              STRING    COMMENT 'Energy method param (only set for electricity_history_day, null otherwise)',
    measurement_date           STRING    COMMENT 'Date of the data (YYYY-MM-DD)',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp, converted from epoch',
    value                      STRING    COMMENT 'Measured value stored as string (cast to numeric in Silver)',
    source_file                STRING    COMMENT 'Source JSON filename for lineage',
    bronze_processing_timestamp TIMESTAMP COMMENT 'When this record was written to bronze'
)
CLUSTER BY AUTO
COMMENT 'Exploded Ecosphere history measurements at 15-min granularity. Covers electricity, heat, water, gas, and FCR.'
""")

# --- bronze.ecosphere_meterreadings ---
# One row per individual meter per building per energy type.
# Populated from: electricity_meterreadings, heat_meterreadings, water_meterreadings.
# Source shape: [{meter_name, sensor_uuid, datetime, value, energy_method?, tariff?}, ...]
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_METERREADINGS} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    endpoint_name              STRING    COMMENT 'e.g., electricity_meterreadings, heat_meterreadings',
    measurement_date           STRING    COMMENT 'Date of the data (YYYY-MM-DD)',
    sensor_uuid                STRING    COMMENT 'UUID of the individual meter/sensor',
    meter_name                 STRING    COMMENT 'Human-readable meter name (e.g., Hoofdmeter)',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the reading, converted from epoch (nullable)',
    value                      STRING    COMMENT 'Cumulative meter reading stored as string',
    energy_method              STRING    COMMENT 'Flow direction (delivery, return_delivery, consumption) — electricity only',
    tariff                     STRING    COMMENT 'Tariff info — electricity only, usually null',
    source_file                STRING    COMMENT 'Source JSON filename for lineage',
    bronze_processing_timestamp TIMESTAMP COMMENT 'When this record was written to bronze'
)
CLUSTER BY AUTO
COMMENT 'Exploded Ecosphere meter readings. Each row is one individual meter snapshot per building and energy type.'
""")

# --- bronze.ecosphere_point_value ---
# One row per building per endpoint call.
# Populated from: electricity_soc, solar_irradiance.
# Source shape: {datetime, value}  (single object, no array)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_POINT_VALUE} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    endpoint_name              STRING    COMMENT 'e.g., electricity_soc, solar_irradiance',
    measurement_date           STRING    COMMENT 'Date of the data (YYYY-MM-DD)',
    measurement_timestamp      TIMESTAMP COMMENT 'Point-in-time timestamp, converted from epoch',
    value                      STRING    COMMENT 'Measured value stored as string',
    source_file                STRING    COMMENT 'Source JSON filename for lineage',
    bronze_processing_timestamp TIMESTAMP COMMENT 'When this record was written to bronze'
)
CLUSTER BY AUTO
COMMENT 'Ecosphere single point-in-time measurements (battery SOC, solar irradiance). One row per API call per building.'
""")

# --- bronze.ecosphere_ev_socket ---
# One row per EV charger socket per building.
# Populated from: ev_socket.
# Source shape: [{sensor_uuid, datetime, available, in_session}, ...]
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_EV_SOCKET} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    endpoint_name              STRING    COMMENT 'ev_socket',
    measurement_date           STRING    COMMENT 'Date of the data (YYYY-MM-DD)',
    sensor_uuid                STRING    COMMENT 'UUID of the individual EV charger socket',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the status reading, converted from epoch',
    available                  STRING    COMMENT 'Socket availability: yes or no',
    in_session                 STRING    COMMENT 'Active charging session: yes or no',
    source_file                STRING    COMMENT 'Source JSON filename for lineage',
    bronze_processing_timestamp TIMESTAMP COMMENT 'When this record was written to bronze'
)
CLUSTER BY AUTO
COMMENT 'Ecosphere EV charger socket status snapshots. One row per socket per API call per building.'
""")

print("All 4 bronze tables created/verified.")

# COMMAND ----------

# DBTITLE 1,Define JSON Envelope Schema
# b01 writes a uniform JSON envelope for every endpoint:
# {
#   "building_uuid": "...",
#   "building_name": "...",
#   "endpoint_name": "electricity_history_day",
#   "energy_method": "delivery",    <- null for non-electricity endpoints
#   "date": "2026-02-18",
#   "raw_response": { ... }         <- the original API JSON (dict or list depending on endpoint)
# }
#
# raw_response is serialized as a JSON string inside the outer JSON file.
# We read the envelope fields with a fixed schema and handle raw_response as STRING,
# then parse and explode it differently per bronze group.
envelope_schema = StructType([
    StructField("building_uuid", StringType(), True),
    StructField("building_name", StringType(), True),
    StructField("endpoint_name", StringType(), True),
    StructField("energy_method",  StringType(), True),
    StructField("date",           StringType(), True),
    # raw_response is read as STRING — we parse it in the foreachBatch transform
    StructField("raw_response",   StringType(), True),
])

# Schemas for parsing raw_response per bronze group

# History: {start_datetime: long, end_datetime: long, values: [{datetime: long, value: double}]}
history_value_schema = ArrayType(StructType([
    StructField("datetime", LongType(), True),
    StructField("value", DoubleType(), True),
]))

# Meterreadings: [{meter_name, sensor_uuid, datetime, value, energy_method, tariff}]
meterreadings_schema = ArrayType(StructType([
    StructField("meter_name",    StringType(), True),
    StructField("sensor_uuid",   StringType(), True),
    StructField("datetime",      LongType(),   True),
    StructField("value",         DoubleType(), True),
    StructField("energy_method", StringType(), True),
    StructField("tariff",        StringType(), True),
]))

# Point value: {datetime: long, value: double}
point_value_schema = StructType([
    StructField("datetime", LongType(),   True),
    StructField("value",    DoubleType(), True),
])

# EV socket: [{sensor_uuid, datetime, available, in_session}]
ev_socket_schema = ArrayType(StructType([
    StructField("sensor_uuid", StringType(), True),
    StructField("datetime",    LongType(),   True),
    StructField("available",   StringType(), True),
    StructField("in_session",  StringType(), True),
]))

# COMMAND ----------

# DBTITLE 1,Define Transform Functions per Bronze Group

def transform_history(batch_df):
    """
    Parses history envelope rows and explodes the values[] array.
    Source shape: {start_datetime, end_datetime, values: [{datetime, value}]}
    Output: one row per 15-min interval per building.
    """
    return (
        batch_df
        .withColumn("parsed", F.from_json(F.col("raw_response"), StructType([
            StructField("values", history_value_schema, True),
        ])))
        .withColumn("interval", F.explode(F.col("parsed.values")))
        .select(
            F.col("building_uuid"),
            F.col("building_name"),
            F.col("endpoint_name"),
            F.col("energy_method"),
            F.col("date").alias("measurement_date"),
            F.from_unixtime(F.col("interval.datetime")).cast("timestamp").alias("measurement_timestamp"),
            F.col("interval.value").cast("string").alias("value"),
            F.col("source_file"),
            F.current_timestamp().alias("bronze_processing_timestamp"),
        )
        .filter(F.col("measurement_timestamp").isNotNull())
    )


def transform_meterreadings(batch_df):
    """
    Parses meterreadings envelope rows and explodes the top-level array.
    Source shape: [{meter_name, sensor_uuid, datetime, value, energy_method?, tariff?}]
    Output: one row per individual meter reading per building.
    """
    return (
        batch_df
        .withColumn("readings", F.from_json(F.col("raw_response"), meterreadings_schema))
        .withColumn("reading", F.explode(F.col("readings")))
        .select(
            F.col("building_uuid"),
            F.col("building_name"),
            F.col("endpoint_name"),
            F.col("date").alias("measurement_date"),
            F.col("reading.sensor_uuid").alias("sensor_uuid"),
            F.col("reading.meter_name").alias("meter_name"),
            F.from_unixtime(F.col("reading.datetime")).cast("timestamp").alias("measurement_timestamp"),
            F.col("reading.value").cast("string").alias("value"),
            F.col("reading.energy_method").alias("energy_method"),
            F.col("reading.tariff").alias("tariff"),
            F.col("source_file"),
            F.current_timestamp().alias("bronze_processing_timestamp"),
        )
        .filter(F.col("sensor_uuid").isNotNull())
    )


def transform_point_value(batch_df):
    """
    Parses point value envelope rows — no explosion needed, single object.
    Source shape: {datetime, value}
    Output: one row per building per API call.
    """
    return (
        batch_df
        .withColumn("parsed", F.from_json(F.col("raw_response"), point_value_schema))
        .select(
            F.col("building_uuid"),
            F.col("building_name"),
            F.col("endpoint_name"),
            F.col("date").alias("measurement_date"),
            F.from_unixtime(F.col("parsed.datetime")).cast("timestamp").alias("measurement_timestamp"),
            F.col("parsed.value").cast("string").alias("value"),
            F.col("source_file"),
            F.current_timestamp().alias("bronze_processing_timestamp"),
        )
        .filter(F.col("measurement_timestamp").isNotNull())
    )


def transform_ev_socket(batch_df):
    """
    Parses EV socket envelope rows and explodes the top-level array.
    Source shape: [{sensor_uuid, datetime, available, in_session}]
    Output: one row per charger socket per building.
    """
    return (
        batch_df
        .withColumn("sockets", F.from_json(F.col("raw_response"), ev_socket_schema))
        .withColumn("socket", F.explode(F.col("sockets")))
        .select(
            F.col("building_uuid"),
            F.col("building_name"),
            F.col("endpoint_name"),
            F.col("date").alias("measurement_date"),
            F.col("socket.sensor_uuid").alias("sensor_uuid"),
            F.from_unixtime(F.col("socket.datetime")).cast("timestamp").alias("measurement_timestamp"),
            F.col("socket.available").alias("available"),
            F.col("socket.in_session").alias("in_session"),
            F.col("source_file"),
            F.current_timestamp().alias("bronze_processing_timestamp"),
        )
        .filter(F.col("sensor_uuid").isNotNull())
    )

# COMMAND ----------

# DBTITLE 1,foreachBatch Handler — Routes Each Micro-Batch to the Right Bronze Table
def process_batch(batch_df, epoch_id):
    """
    Called once per micro-batch by the Auto Loader stream.
    Filters by endpoint_name group and writes to the corresponding bronze table.
    """
    print(f"\n--- Batch {epoch_id} ---")

    if batch_df.isEmpty():
        print("  Empty batch, skipping.")
        return

    # Cache the batch to avoid re-reading for each group filter
    batch_df.cache()

    # --- History group ---
    history_df = batch_df.filter(F.col("endpoint_name").isin(HISTORY_ENDPOINTS))
    if not history_df.isEmpty():
        transformed = transform_history(history_df)
        rows = transformed.count()
        transformed.write.format("delta").mode("append").saveAsTable(BRONZE_HISTORY)
        print(f"  [history]      -> {rows:,} rows written to {BRONZE_HISTORY}")

    # --- Meterreadings group ---
    meter_df = batch_df.filter(F.col("endpoint_name").isin(METERREADINGS_ENDPOINTS))
    if not meter_df.isEmpty():
        transformed = transform_meterreadings(meter_df)
        rows = transformed.count()
        transformed.write.format("delta").mode("append").saveAsTable(BRONZE_METERREADINGS)
        print(f"  [meterreadings] -> {rows:,} rows written to {BRONZE_METERREADINGS}")

    # --- Point value group ---
    point_df = batch_df.filter(F.col("endpoint_name").isin(POINT_VALUE_ENDPOINTS))
    if not point_df.isEmpty():
        transformed = transform_point_value(point_df)
        rows = transformed.count()
        transformed.write.format("delta").mode("append").saveAsTable(BRONZE_POINT_VALUE)
        print(f"  [point_value]  -> {rows:,} rows written to {BRONZE_POINT_VALUE}")

    # --- EV socket group ---
    ev_df = batch_df.filter(F.col("endpoint_name").isin(EV_SOCKET_ENDPOINTS))
    if not ev_df.isEmpty():
        transformed = transform_ev_socket(ev_df)
        rows = transformed.count()
        transformed.write.format("delta").mode("append").saveAsTable(BRONZE_EV_SOCKET)
        print(f"  [ev_socket]    -> {rows:,} rows written to {BRONZE_EV_SOCKET}")

    batch_df.unpersist()

# COMMAND ----------

# DBTITLE 1,Read All JSON Files with Auto Loader
# Single stream reads all JSON files from the entire volume tree recursively.
# The foreachBatch handler routes each row to the correct bronze table.
# The checkpoint tracks which files have been processed — safe to re-run.
raw_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useIncrementalListing", "auto")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .option("multiLine", "true")
        .option("pathGlobFilter", "*.json")
        .option("recursiveFileLookup", "true")
        .schema(envelope_schema)
        .load(VOLUME_PATH)
    .withColumn("source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
)

# COMMAND ----------

# DBTITLE 1,Run Stream
# We use the history checkpoint as the primary checkpoint for the single stream.
# This is fine because there is only one stream reading the volume — the checkpoint
# tracks file processing, not which table was written.
bronze_query = (
    raw_stream_df
        .writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_HISTORY)
        .trigger(availableNow=True)
        .start()
)

bronze_query.awaitTermination()
print("\nStream completed.")

# COMMAND ----------

# DBTITLE 1,Verify Row Counts
print(f"\n{'='*60}")
print(f"BRONZE LAYER SUMMARY")
print(f"{'='*60}")

tables = {
    "ecosphere_batch_history":       BRONZE_HISTORY,
    "ecosphere_batch_meterreadings": BRONZE_METERREADINGS,
    "ecosphere_batch_point_value":   BRONZE_POINT_VALUE,
    "ecosphere_batch_ev_socket":     BRONZE_EV_SOCKET,
}

for label, table_path in tables.items():
    try:
        total = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_path}").collect()[0]["cnt"]
        print(f"\n{label}: {total:,} total rows")
        spark.sql(f"""
            SELECT endpoint_name, COUNT(*) as cnt
            FROM {table_path}
            GROUP BY endpoint_name
            ORDER BY endpoint_name
        """).show(truncate=False)
    except Exception as e:
        print(f"  ERROR reading {table_path}: {e}")
