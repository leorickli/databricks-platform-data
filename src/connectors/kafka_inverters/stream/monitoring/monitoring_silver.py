# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Widget to control initial position:
# - TRIM_HORIZON: Read all available data (for testing/backfill)
# - LATEST: Only read new data (for real-time monitoring when pipeline is continuously running)
dbutils.widgets.dropdown("initial_position", "TRIM_HORIZON", ["LATEST", "TRIM_HORIZON"], "Initial Position")
INITIAL_POSITION = dbutils.widgets.get("initial_position")

SILVER_MONITORING_TABLE_NAME = "monitoring_kafka_inverters_silver_stream"
SILVER_MONITORING_TABLE = f"{CATALOG_NAME}.operational.{SILVER_MONITORING_TABLE_NAME}"

AWS_REGION = "eu-central-1"
KINESIS_SOURCE_STREAM_NAME = "dpx-kinesis-globex-silver"
KINESIS_IAM_ROLE_ARN = "arn:aws:iam::867344430965:role/dpx-databricks-kinesis-role"

CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/{SILVER_MONITORING_TABLE_NAME}"

print(f"Configuration:")
print(f"  Kinesis Stream: {KINESIS_SOURCE_STREAM_NAME}")
print(f"  Initial Position: {INITIAL_POSITION}")
print(f"  Checkpoint: {CHECKPOINT_LOCATION}")
print(f"  Target Table: {SILVER_MONITORING_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read from Kinesis Stream
kinesis_df = (
    spark
    .readStream
    .format("kinesis")
    .option("streamName", KINESIS_SOURCE_STREAM_NAME)
    .option("region", AWS_REGION)
    .option("roleArn", KINESIS_IAM_ROLE_ARN)
    .option("initialPosition", INITIAL_POSITION)
    .option("awsSTSRoleSessionName", "databricks-kinesis-session")
    .load()
)

# COMMAND ----------

# DBTITLE 1,Define Source Schema
# Silver layer schema - Haystack-compliant inverter measurements
silver_schema = StructType([
    StructField("id", StringType(), True),
    StructField("idSite", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ingestionTimestamp", StringType(), True),
    StructField("W", StringType(), True),  # Read as string first due to Decimal serialization
    StructField("WH", StringType(), True),
    StructField("dailyWH", StringType(), True),
    StructField("PhVphA", StringType(), True),
    StructField("Hz", StringType(), True),
    StructField("TmpCab", StringType(), True),
    StructField("operatingMode", StringType(), True),
    StructField("silverProcessingTimestamp", StringType(), True),
    StructField("kinesis_export_timestamp", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Parse and Transform Data
parsed_df = (
    kinesis_df
    .select(
        col("data").cast("string").alias("json_data"),
        col("approximateArrivalTimestamp"),
        col("partitionKey")
    )
    .select(
        from_json(col("json_data"), silver_schema).alias("parsed"),
        col("approximateArrivalTimestamp")
    )
    .select(
        col("parsed.id"),
        col("parsed.idSite"),
        col("parsed.timestamp"),
        col("parsed.ingestionTimestamp"),
        col("parsed.W").cast("double").alias("W"),
        col("parsed.WH").cast("double").alias("WH"),
        col("parsed.dailyWH").cast("double").alias("dailyWH"),
        col("parsed.PhVphA").cast("double").alias("PhVphA"),
        col("parsed.Hz").cast("double").alias("Hz"),
        col("parsed.TmpCab").cast("double").alias("TmpCab"),
        col("parsed.operatingMode").cast("double").alias("operatingMode"),
        col("parsed.silverProcessingTimestamp"),
        col("parsed.kinesis_export_timestamp"),
        current_timestamp().cast("string").alias("monitoring_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,Create Silver Monitoring Table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_MONITORING_TABLE} (
        id STRING COMMENT 'Unique identifier for the device (Haystack: id)',
        idSite STRING COMMENT 'Reference to parent site entity (Haystack: siteRef, materialized as idSite)',
        timestamp STRING COMMENT 'The originating timestamp of the event, windowed to the minute (Haystack: ts)',
        ingestionTimestamp STRING COMMENT 'The timestamp from the initial batch ingestion into the land layer',
        W DOUBLE COMMENT 'AC Power output in Watts (Haystack: W)',
        WH DOUBLE COMMENT 'Total AC Energy yield in Watt-hours (Haystack: WH)',
        dailyWH DOUBLE COMMENT 'Daily energy yield in Watt-hours (Custom field: dailyWH)',
        PhVphA DOUBLE COMMENT 'Phase Voltage AN in Volts (Haystack: PhVphA)',
        Hz DOUBLE COMMENT 'Line Frequency in Hertz (Haystack: Hz)',
        TmpCab DOUBLE COMMENT 'Cabinet Temperature in Celsius (Haystack: TmpCab)',
        operatingMode DOUBLE COMMENT 'Device operating mode code (Custom field: operatingMode)',
        silverProcessingTimestamp STRING COMMENT 'Timestamp when the record was processed into the silver table',
        kinesis_export_timestamp STRING COMMENT 'Timestamp when the record was exported to Kinesis',
        monitoring_timestamp STRING COMMENT 'Timestamp when this monitoring table processed the record from Kinesis'
    ) CLUSTER BY AUTO
    COMMENT 'Monitoring table for the silver layer Haystack-compliant inverter metrics from the GLOBEX Kafka inverters Kinesis stream';
""")

# COMMAND ----------

# DBTITLE 1,Write to Silver Monitoring Delta Table
silver_query = (
    parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .trigger(processingTime = "1 seconds")
    .toTable(SILVER_MONITORING_TABLE)
)
