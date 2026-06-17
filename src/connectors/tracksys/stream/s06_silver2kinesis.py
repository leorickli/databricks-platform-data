# Databricks notebook source
# DBTITLE 1,Imports and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("silver_table_name", "tracksys_stream", "Silver table name")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_TABLE = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

AWS_REGION = "eu-central-1"
KINESIS_SINK_STREAM_NAME = "dpx-kinesis-acme-silver"

CHECKPOINT_LOCATION = "s3://dpx-s3-dev/acme/checkpoints/stream/silver_kinesis_export"

# COMMAND ----------

# DBTITLE 1,Kinesis Writer Class
class SilverKinesisWriter:
    """Foreach writer for streaming Silver data to Kinesis"""
    
    def open(self, partition_id, epoch_id):
        """Initialize Kinesis client for this partition"""
        import boto3
        self.kinesis = boto3.client('kinesis', region_name=AWS_REGION)
        self.stream_name = KINESIS_SINK_STREAM_NAME
        return True

    def process(self, row):
        """Process each row and send to Kinesis"""
        import json
        from decimal import Decimal
        from datetime import datetime
        
        try:
            # Convert row to dictionary
            data = row.asDict(recursive=True)
            
            # Handle special data types for JSON serialization
            for key, value in data.items():
                if value is None:
                    continue
                elif isinstance(value, Decimal):
                    # Convert Decimal to float for JSON
                    data[key] = float(value)
                elif isinstance(value, datetime):
                    # Convert datetime to ISO format string
                    data[key] = value.isoformat()
                elif not isinstance(value, (str, int, float, bool, list, dict)):
                    # Convert any other types to string
                    data[key] = str(value)
            
            # Add export timestamp
            data['kinesis_export_timestamp'] = datetime.now().isoformat()
            
            # Use logger IMEI as partition key for consistent routing
            partition_key = str(data.get('loggerImei', 'default'))
            
            # Send to Kinesis
            self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            
        except Exception as e:
            print(f"Error writing Silver record to Kinesis: {str(e)}")
            print(f"Problematic record: {row}")
            # In production, consider implementing retry logic or dead letter queue

    def close(self, error):
        """Clean up resources"""
        if error:
            print(f"Error in Silver Kinesis writer: {error}")

# COMMAND ----------

# DBTITLE 1,Write Silver and Process to Kinesis Stream
silver_stream = (
    spark.readStream
    .format("delta")
    .option("startingVersion", "latest")
    .table(SILVER_TABLE)
)

silver_kinesis_export = (
    silver_stream
    .writeStream
    .foreach(SilverKinesisWriter())
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="1 seconds")
    .start()
)