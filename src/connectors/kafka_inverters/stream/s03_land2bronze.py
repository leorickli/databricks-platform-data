# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, TimestampType, ArrayType, StructType, StructField

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "kafka_inverters_stream", "Land Volume Name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

dbutils.widgets.text("bronze_table_name", "kafka_inverters_stream", "Bronze Table Name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/kafka_inverters_bronze_stream"

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PATH} (
        -- Batch metadata
        ingestion_timestamp TIMESTAMP COMMENT 'Timestamp of when the batch file was created and ingested into the landing zone',
        source_topic STRING COMMENT 'The source Kafka topic from which the messages were consumed',
        consumer_group STRING COMMENT 'The Kafka consumer group ID used to fetch the messages',
        batch_file_path STRING COMMENT 'Full path to the source JSON file in the landing volume for lineage',
        
        -- Message fields
        project_id STRING COMMENT 'Identifier for the overall project, e.g., "Inverters"',
        application_id STRING COMMENT 'Identifier for the specific application or data source within the project',
        device_id STRING COMMENT 'Unique identifier for the specific device sending the data',
        timestamp STRING COMMENT 'Original event timestamp from the device, as a Unix epoch in milliseconds',
        project_description STRING COMMENT 'Descriptive name for the project',
        application_description STRING COMMENT 'Descriptive name for the application',
        device_description STRING COMMENT 'Descriptive name for the device',
        device_manufacturer STRING COMMENT 'Manufacturer of the device',
        device_type STRING COMMENT 'Model or type of the device',
        device_serial STRING COMMENT 'Serial number of the device',
        location_id STRING COMMENT 'Identifier for the physical location of the device',
        location_description STRING COMMENT 'Descriptive name for the location',
        latitude STRING COMMENT 'GPS latitude of the device',
        longitude STRING COMMENT 'GPS longitude of the device',
        altitude STRING COMMENT 'Altitude of the device in meters',
        
        -- Measurements as JSON string
        measurements STRING COMMENT 'A JSON string containing an array of measurements from the device',
        
        -- Processing metadata
        bronze_processing_timestamp TIMESTAMP COMMENT 'When the record was processed in the bronze layer'
    )
    CLUSTER BY AUTO
    COMMENT 'Bronze table containing raw data ingested from the Kafka topic for solar inverters. Each record represents a single message, with the original measurements stored as a JSON string. Includes batch metadata for traceability.'
""")

# COMMAND ----------

measurements_schema = ArrayType(
    StructType([
        StructField("measurement_id", StringType(), True),
        StructField("value", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("measurement_description", StringType(), True)
    ])
)

input_schema = StructType([
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_topic", StringType(), True),
    StructField("consumer_group", StringType(), True),
    StructField("message_count", LongType(), True),
    StructField("messages", ArrayType(
        StructType([
            StructField("project_id", StringType(), True),
            StructField("application_id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("project_description", StringType(), True),
            StructField("application_description", StringType(), True),
            StructField("device_description", StringType(), True),
            StructField("device_manufacturer", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("device_serial", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("location_description", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("altitude", StringType(), True),
            StructField("measurements", measurements_schema, True)
        ])
    ), True)
])

# COMMAND ----------

# DBTITLE 1,Define Autoloader Stream
df_stream = (
    spark.readStream
    .schema(input_schema)
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.maxFilesPerTrigger", 100)
    .option("recursiveFileLookup", "true")
    # Reads the entire file as one single JSON object
    .option("multiLine", "true")
    # Handles "NaN" values
    .option("allowNonNumericNumbers", "true")
    .option("pathGlobFilter", "*.json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_LOCATION)
    .load(VOLUME_PATH)
)

# COMMAND ----------

# DBTITLE 1,Transform Data for Bronze Layer
df_transformed = (
    df_stream
    # Add file path for lineage
    .withColumn("batch_file_path", F.col("_metadata.file_path"))
    
    # Explode the messages array to get individual records
    .select(
        F.col("ingestion_timestamp"),
        F.col("source_topic"),
        F.col("consumer_group"),
        F.col("batch_file_path"),
        F.explode(F.col("messages")).alias("message")
    )
    
    # Extract fields from the message
    .select(
        # Batch metadata
        F.col("ingestion_timestamp"),
        F.col("source_topic"),
        F.col("consumer_group"),
        F.col("batch_file_path"),
        
        # Message fields
        F.col("message.project_id").cast("string").alias("project_id"),
        F.col("message.application_id").cast("string").alias("application_id"),
        F.col("message.device_id").cast("string").alias("device_id"),
        F.col("message.timestamp").cast("string").alias("timestamp"),
        F.col("message.project_description").cast("string").alias("project_description"),
        F.col("message.application_description").cast("string").alias("application_description"),
        F.col("message.device_description").cast("string").alias("device_description"),
        F.col("message.device_manufacturer").cast("string").alias("device_manufacturer"),
        F.col("message.device_type").cast("string").alias("device_type"),
        F.col("message.device_serial").cast("string").alias("device_serial"),
        F.col("message.location_id").cast("string").alias("location_id"),
        F.col("message.location_description").cast("string").alias("location_description"),
        F.col("message.latitude").cast("string").alias("latitude"),
        F.col("message.longitude").cast("string").alias("longitude"),
        F.col("message.altitude").cast("string").alias("altitude"),
        
        # Keep measurements as JSON string
        F.to_json(F.col("message.measurements")).alias("measurements"),
        
        # Add processing timestamp
        F.current_timestamp().alias("bronze_processing_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,Write Stream to Bronze Table
query = (
    df_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="5 seconds")
    .toTable(BRONZE_PATH)
)