# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# --- Catalog and Table Configuration ---
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "kafka_inverters_stream", "Bronze Source Table")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

# --- Kinesis Configuration ---
# Set explicit defaults in the get() call for safety in Job contexts
dbutils.widgets.text("kinesis_stream_name", "dpx-kinesis-globex-bronze", "Target Kinesis Stream Name")
KINESIS_STREAM_NAME = dbutils.widgets.get("kinesis_stream_name") or "dpx-kinesis-globex-bronze"

dbutils.widgets.text("aws_region", "eu-central-1", "AWS Region")
AWS_REGION = dbutils.widgets.get("aws_region") or "eu-central-1"

# Checkpoint for this specific stream to Kinesis
CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/kafka_inverters_bronze_kinesis_sink"

print(f"Reading from: {BRONZE_PATH}")
print(f"Writing to Kinesis Stream: {KINESIS_STREAM_NAME} in {AWS_REGION}")
print(f"Checkpoint: {CHECKPOINT_LOCATION}")

# COMMAND ----------

# DBTITLE 1,Read from Bronze Table
# Read the Delta table as a stream
df_bronze_stream = spark.readStream.table(BRONZE_PATH)

# COMMAND ----------

# DBTITLE 1,Transform for Kinesis (Serialize to JSON)
# The Kinesis sink expects exactly two columns:
# 1. partitionKey (String): Determines which shard the data goes to.
# 2. data (Binary): The actual payload.

df_kinesis_prepared = (
    df_bronze_stream
    # Use device_id as partitionKey to ensure ordering per device
    # If device_id is null, fallback to a random UUID or static string
    .withColumn("partitionKey", 
                F.when(F.col("device_id").isNotNull(), F.col("device_id"))
                 .otherwise(F.lit("unknown_device"))
    )
    # Serialize the entire row struct into a JSON string, then cast to Binary
    .withColumn("data", F.to_json(F.struct(F.col("*"))).cast("binary"))
    # Select only the required columns
    .select("partitionKey", "data")
)

# COMMAND ----------

# DBTITLE 1,Write Stream to Kinesis (foreachBatch + boto3)
def write_to_kinesis_batch(batch_df, batch_id):
    """
    Writes a micro-batch to Kinesis using boto3 directly on workers.
    This bypasses the 'KinesisSourceProvider does not allow create table as select' error
    and works reliably with foreachBatch/availableNow.
    """
    if batch_df.isEmpty():
        return
        
    # [CRITICAL] Capture global variables into local scope for closure serialization
    target_region = AWS_REGION
    target_stream = KINESIS_STREAM_NAME
    
    # We use foreachPartition (Action) instead of mapPartitions (Transformation)
    # because we are performing a side-effect (writing) and not returning data.
    def send_partition_to_kinesis(partition_iterator):
        import boto3
        import traceback
        import sys
        
        # Initialize Kinesis client
        # Note: Ensure the Job Cluster Instance Profile has 'kinesis:PutRecords' permission.
        try:
            # [FIX] Explicitly use EC2 instance metadata for credentials.
            # In Databricks Runtime 17.x on job clusters, boto3 tries to use
            # Databricks service credentials by default, which fails.
            # We force it to use the EC2 instance profile instead.
            from botocore.credentials import InstanceMetadataProvider
            from botocore.utils import InstanceMetadataFetcher

            # Create instance metadata provider
            provider = InstanceMetadataProvider(
                iam_role_fetcher=InstanceMetadataFetcher(timeout=1, num_attempts=2)
            )

            # Get credentials from instance profile
            credentials = provider.load()

            if credentials is None:
                raise RuntimeError("Failed to load credentials from instance profile")

            # Create Kinesis client with explicit instance profile credentials
            kinesis_client = boto3.client(
                'kinesis',
                region_name=target_region,
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_session_token=credentials.token
            )
            
        except Exception as e:
            # Print to stderr to ensure it shows up in driver/executor logs
            print(f"FATAL: Could not initialize Kinesis client in region {target_region}.", file=sys.stderr)
            traceback.print_exc()
            raise e
        
        # Kinesis PutRecords limits: 500 records or 5MB per call
        MAX_RECORDS = 500
        MAX_BYTES = 4.5 * 1024 * 1024  # 4.5 MB safety limit
        
        records_batch = []
        current_batch_size = 0
        
        try:
            for row in partition_iterator:
                p_key = row['partitionKey']
                data = row['data']
                
                # Calculate size contribution (Data + PartitionKey)
                # UTF-8 length estimation for partition key
                item_size = len(data) + len(p_key.encode('utf-8'))
                
                # Flush if limits reached
                if (len(records_batch) >= MAX_RECORDS) or (current_batch_size + item_size > MAX_BYTES):
                    if records_batch:
                        kinesis_client.put_records(StreamName=target_stream, Records=records_batch)
                    
                    records_batch = []
                    current_batch_size = 0
                
                # Add to batch
                records_batch.append({'Data': data, 'PartitionKey': p_key})
                current_batch_size += item_size
                
            # Flush remaining records
            if records_batch:
                kinesis_client.put_records(StreamName=target_stream, Records=records_batch)
                
        except Exception as e:
            print(f"ERROR: Failed to write records to Kinesis stream {target_stream}", file=sys.stderr)
            traceback.print_exc()
            raise e

    # Trigger the action using foreachPartition
    # This executes the function on each partition of the DataFrame
    batch_df.rdd.foreachPartition(send_partition_to_kinesis)

query = (
    df_kinesis_prepared.writeStream
    .foreachBatch(write_to_kinesis_batch)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="5 seconds")
    .start()
)

# Wait for the stream to finish processing all available data
# query.awaitTermination()