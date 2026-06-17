# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from delta.tables import DeltaTable

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "kafka_inverters_stream", "Bronze Source Table")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

dbutils.widgets.text("silver_table_name", "kafka_inverters_stream", "Silver Target Table")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_PATH = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/kafka_inverters_silver_stream"

# COMMAND ----------

# DBTITLE 1,Read from Bronze Table
bronze_stream_df = spark.readStream.table(BRONZE_PATH)

# COMMAND ----------

# DBTITLE 1,Create Silver Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_PATH} (
    -- Haystack-compliant identifier and reference fields
    id STRING COMMENT 'Unique identifier for the device (Haystack: id)',
    idSite STRING COMMENT 'Reference to parent site entity (Haystack: siteRef, materialized as idSite)',

    -- Timestamp fields
    timestamp TIMESTAMP COMMENT 'The originating timestamp of the event, windowed to the minute (Haystack: ts)',
    ingestionTimestamp TIMESTAMP COMMENT 'The timestamp from the initial batch ingestion into the land layer',

    -- Haystack-compliant inverter measurements (SunSpec aligned)
    W DECIMAL(18, 2) COMMENT 'AC Power output in Watts (Haystack: W)',
    WH DECIMAL(18, 2) COMMENT 'Total AC Energy yield in Watt-hours (Haystack: WH)',
    dailyWH DECIMAL(18, 2) COMMENT 'Daily energy yield in Watt-hours (Custom field: dailyWH)',
    PhVphA DECIMAL(18, 2) COMMENT 'Phase Voltage AN in Volts (Haystack: PhVphA)',
    Hz DECIMAL(18, 2) COMMENT 'Line Frequency in Hertz (Haystack: Hz)',
    TmpCab DECIMAL(18, 2) COMMENT 'Cabinet Temperature in Celsius (Haystack: TmpCab)',
    operatingMode DECIMAL(18, 2) COMMENT 'Device operating mode code (Custom field: operatingMode)',

    -- Silver layer processing metadata
    silverProcessingTimestamp TIMESTAMP COMMENT 'Timestamp when the record was processed into the silver table'
)
CLUSTER BY AUTO
COMMENT 'Silver table containing Haystack ontology-compliant measurements for SMA solar inverters.'
""")

# COMMAND ----------

# DBTITLE 1,Define Target Measurements and Schema (Haystack-compliant)
target_measurements = {
    "Power Phase L1": ("W", "DOUBLE"),  # AC Power (Haystack: W)
    "Total Yield": ("WH", "DOUBLE"),  # AC Energy (Haystack: WH)
    "Daily Yield": ("dailyWH", "DOUBLE"),  # Daily energy yield (Custom field)
    "Grid Voltage Phase L1N": ("PhVphA", "DOUBLE"),  # Phase Voltage AN (Haystack: PhVphA)
    "Grid Frequency": ("Hz", "DOUBLE"),  # Line Frequency (Haystack: Hz)
    "Internal Temperature": ("TmpCab", "DOUBLE"),  # Cabinet Temperature (Haystack: TmpCab)
    "General Operating Mode": ("operatingMode", "DOUBLE")  # Operating mode code (Custom field)
}

# Schema to parse the 'measurements' JSON string from the bronze table
json_schema = ArrayType(StructType([
    StructField("measurement_id", StringType(), True),
    StructField("value", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("measurement_description", StringType(), True)
]))

# COMMAND ----------

# DBTITLE 1,Apply Transformations
# Convert timestamp, parse JSON, and explode
unpacked_df = bronze_stream_df \
    .withColumn("timestamp", (F.col("timestamp") / 1000).cast("timestamp")) \
    .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast("timestamp")) \
    .withColumn("measurements_parsed", F.from_json(F.col("measurements"), json_schema)) \
    .withColumn("measurement", F.explode(F.col("measurements_parsed")))

# Define the columns that uniquely identify each original message
grouping_cols = ["ingestion_timestamp", "device_id", "location_id"]

# Pivot the data: turn measurement_id rows into columns.
pivoted_df = unpacked_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(*grouping_cols, F.window("timestamp", "1 minute")) \
    .pivot("measurement.measurement_id", list(target_measurements.keys())) \
    .agg(F.first("measurement.value"))

# Select the final columns, casting them to the correct types and renaming to Haystack-compliant names
select_expr = [
    F.col("window.start").alias("timestamp"),
    F.col("ingestion_timestamp").alias("ingestionTimestamp"),
    F.col("device_id").alias("id"),
    F.col("location_id").alias("idSite")
]

for original_name, (target_name, dtype) in target_measurements.items():
    # Cast the pivoted column's value to double first to handle all values as numbers
    col_expr = F.col(f"`{original_name}`").cast("double")
    
    # Cast all numeric measurements to a decimal type to enforce two decimal places.
    final_col_expr = col_expr.cast("decimal(18, 2)").alias(target_name)
        
    select_expr.append(final_col_expr)

# Select the columns and add the processed timestamp
final_silver_df = pivoted_df \
    .select(*select_expr) \
    .withColumn("silverProcessingTimestamp", F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Define Upsert Function for foreachBatch
def upsert_to_silver(micro_batch_df, batch_id):
    silver_table = DeltaTable.forName(spark, SILVER_PATH)

    (silver_table.alias("target")
     .merge(micro_batch_df.alias("source"), "target.id = source.id AND target.timestamp = source.timestamp")
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

# DBTITLE 1,Write Stream to Silver Table
query = (
    final_silver_df.writeStream
    .foreachBatch(upsert_to_silver)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="5 seconds") 
    .start()
)