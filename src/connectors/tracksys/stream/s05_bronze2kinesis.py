# Databricks notebook source
# DBTITLE 1,Imports and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "tracksys_stream", "Bronze table name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_TABLE = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

AWS_REGION = "eu-central-1"
KINESIS_SINK_STREAM_NAME = "dpx-kinesis-acme-bronze"

CHECKPOINT_LOCATION = "s3://dpx-s3-dev/acme/checkpoints/stream/bronze_kinesis_export"

# COMMAND ----------

# DBTITLE 1,Kinesis Writer Class
class BronzeKinesisWriter:
    """Foreach writer for streaming Bronze data to Kinesis"""
    
    def open(self, partition_id, epoch_id):
        """Initialize Kinesis client for this partition"""
        import boto3
        self.kinesis = boto3.client('kinesis', region_name=AWS_REGION)
        self.stream_name = KINESIS_SINK_STREAM_NAME
        return True

    def process(self, row):
        """Process each row and send to Kinesis"""
        import json
        from datetime import datetime
        
        try:
            # Convert row to dictionary
            data = {}
            for field in row.asDict():
                value = row[field]
                # Convert all values to strings for JSON serialization
                data[field] = str(value) if value is not None else None
            
            # Add export timestamp - MUST use datetime.now(), not current_timestamp()
            data['kinesis_export_timestamp'] = datetime.now().isoformat()
            
            # Use logger_imei as partition key for consistent routing
            partition_key = str(data.get('logger_imei', 'default'))
            
            # Send to Kinesis
            self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            
        except Exception as e:
            print(f"Error writing Bronze record to Kinesis: {str(e)}")
            # In production, consider implementing retry logic or dead letter queue

    def close(self, error):
        """Clean up resources"""
        if error:
            print(f"Error in Bronze Kinesis writer: {error}")

# COMMAND ----------

# DBTITLE 1,Write Bronze and Write to Kinesis Stream
bronze_stream = (
    spark.readStream
    .format("delta")
    .option("startingVersion", "latest")
    .table(BRONZE_TABLE)
)

bronze_kinesis_export = (
    bronze_stream
    .writeStream
    .foreach(BronzeKinesisWriter())
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="1 seconds")
    .start()
)