# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DoubleType, IntegerType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "wattflow_dap_batch", "Volume Name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}/"

dbutils.widgets.text("bronze_table_name", "wattflow_dap_batch", "Bronze Table Name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

CHECKPOINT_PATH = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/{BRONZE_TABLE_NAME}/"

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_PATH} (
    dap_id STRING COMMENT 'The Data Access Point ID from the Wattflow system',
    ean STRING COMMENT 'European Article Number - grid connection identifier',
    aggregation_level STRING COMMENT 'Aggregation level: quarterly (15min), hourly, or daily',
    measurement_datetime TIMESTAMP COMMENT 'The timestamp for this specific measurement interval',
    measurement_date STRING COMMENT 'The date of measurement (extracted from file for partitioning)',

    -- Measurement fields (all stored as strings in bronze layer)
    value STRING COMMENT 'The measured value',
    unit STRING COMMENT 'Unit of measurement (e.g., kWh, kVARh, kWmax)',
    utility_type STRING COMMENT 'Type of utility and direction (e.g., wh-energy consumption, varh-energy production)',
    consumption STRING COMMENT 'Flag indicating if measurement is consumption (true) or production (false)',
    rate STRING COMMENT 'Tariff rate (e.g., low, normal, na)',
    is_max STRING COMMENT 'Flag indicating if this is a peak/max reading',

    -- Processing metadata
    source_file STRING COMMENT 'Source JSON file in landing volume for lineage',
    bronze_processing_timestamp TIMESTAMP COMMENT 'When this record was processed into bronze layer'
)
CLUSTER BY AUTO
COMMENT 'Unpivoted Wattflow energy measurements at various granularities (15min/hourly/daily).'
""")

# COMMAND ----------

# DBTITLE 1,Define JSON Schema
# Schema for the "values" array within each record
measurement_value_schema = StructType([
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("utility_type", StringType(), True),
    StructField("consumption", BooleanType(), True),
    StructField("rate", StringType(), True),
    StructField("is_max", BooleanType(), True)
])

# Schema for each record in the "records" array
record_schema = StructType([
    StructField("dap_id", IntegerType(), True),  # Integer in records array (unlike top-level which is string)
    StructField("datetime", StringType(), True),
    StructField("values", ArrayType(measurement_value_schema), True)
])

# Top-level schema for the entire JSON file
source_schema = StructType([
    StructField("dap_id", StringType(), True),
    StructField("ean", StringType(), True),
    StructField("date", StringType(), True),
    StructField("aggregation_level", StringType(), True),
    StructField("records", ArrayType(record_schema), True)
])

# COMMAND ----------

# DBTITLE 1,Read JSON Files with Auto Loader
raw_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("pathGlobFilter", "*.json")
        .schema(source_schema)
        .load(VOLUME_PATH)
)

# COMMAND ----------

# DBTITLE 1,Transform and Explode Data
# Transform the nested JSON structure into a flat, unpivoted format
transformed_stream_df = (
    raw_stream_df
    # Extract source file name for lineage
    .withColumn("source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))

    # Explode the records array to get one row per time interval
    .withColumn("record", F.explode(F.col("records")))

    # Extract datetime from the exploded record
    .withColumn("measurement_datetime_str", F.col("record.datetime"))

    # Explode the values array within each record to get one row per measurement
    .withColumn("measurement", F.explode(F.col("record.values")))

    # Convert to final structure with proper types
    .select(
        # DAP and aggregation info
        F.col("dap_id").cast("string").alias("dap_id"),
        F.col("ean").cast("string").alias("ean"),
        F.col("aggregation_level").cast("string").alias("aggregation_level"),

        # Timestamps
        F.to_timestamp(F.col("measurement_datetime_str")).alias("measurement_datetime"),
        F.col("date").alias("measurement_date"),

        # Measurement values (convert all to strings for bronze layer)
        F.col("measurement.value").cast("string").alias("value"),
        F.col("measurement.unit").cast("string").alias("unit"),
        F.col("measurement.utility_type").cast("string").alias("utility_type"),
        F.col("measurement.consumption").cast("string").alias("consumption"),
        F.col("measurement.rate").cast("string").alias("rate"),
        F.col("measurement.is_max").cast("string").alias("is_max"),

        # Metadata
        F.col("source_file"),
        F.current_timestamp().alias("bronze_processing_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Table
bronze_query = (
    transformed_stream_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_PATH)
)

# COMMAND ----------

# DBTITLE 1,Wait for Stream to Complete
bronze_query.awaitTermination()
