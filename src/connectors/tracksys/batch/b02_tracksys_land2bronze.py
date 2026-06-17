# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import re
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from typing import Iterator
from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "tracksys_batch", "Volume Name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}/"

dbutils.widgets.text("bronze_table_name", "tracksys_batch", "Bronze Table Name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

CHECKPOINT_PATH = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/{BRONZE_TABLE_NAME}/"

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_PATH} (
    -- Signal data fields
    timestamp TIMESTAMP COMMENT 'Signal measurement time',
    signal_name STRING COMMENT 'Signal identifier',
    signal_value STRING COMMENT 'Measured value',
    signal_unit STRING COMMENT 'Signal unit from CSV header',
    signal_description STRING COMMENT 'Signal description from CSV header',
    
    -- Metadata fields from CSV header
    imei STRING COMMENT 'International Mobile Equipment Identity of the data logger device',
    vin STRING COMMENT 'Vehicle identification number',
    start_time STRING COMMENT 'Trip start time',
    stop_time STRING COMMENT 'Trip end time',
    journey_time STRING COMMENT 'Trip duration in seconds',
    odo_journey STRING COMMENT 'Trip distance in km',
    gps_start_pos STRING COMMENT 'GPS start coordinates',
    gps_stop_pos STRING COMMENT 'GPS end coordinates',
    file_source STRING COMMENT 'Data source system',
    data_section_startrow STRING COMMENT 'Data section start row',
    configuration_name STRING COMMENT 'Logger configuration',
    logger_serialnumber STRING COMMENT 'Logger serial number',
    report_template STRING COMMENT 'Report template used',
    source_file STRING COMMENT 'Source CSV filename',
    
    -- Processing metadata
    processing_timestamp TIMESTAMP COMMENT 'When this record was processed'
) CLUSTER BY AUTO
COMMENT 'Unified telemetry data table combining metadata and signal data.'
""")

# COMMAND ----------

# DBTITLE 1,Define the Parsing Logic as a Pandas UDF
def parse_and_unpivot_file_udf(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    # Process each pandas DataFrame in the iterator
    for pandas_df in iterator:
        # Iterate over each row in the input pandas DataFrame. Each row corresponds to one source file.
        for _, row in pandas_df.iterrows():
            file_path = row['path']
            binary_content = row['content']
            file_name = file_path.split('/')[-1]
            
            # --- Decode the binary content ---
            lines = None
            encodings_to_try = ['cp1252', 'utf-8', 'latin1', 'iso-8859-1']
            for encoding in encodings_to_try:
                try:
                    decoded_content = binary_content.decode(encoding)
                    lines = decoded_content.splitlines()
                    break 
                except (UnicodeDecodeError, AttributeError):
                    continue 
            
            if lines is None:
                print(f"WARNING: Could not decode file {file_name}. Skipping.")
                continue

            # --- Parse metadata and headers ---
            metadata, header_lines, data_lines = {}, [], []
            in_metadata_section, in_data_section = False, False
            
            for line in lines:
                stripped_line = line.strip()
                if not stripped_line or stripped_line.startswith('/'): 
                    continue
                if stripped_line.startswith('\\METADATA:'):
                    in_metadata_section, in_data_section = True, False
                    continue
                elif stripped_line.startswith('\\DATA:'):
                    in_metadata_section, in_data_section = False, True
                    continue
                
                if in_metadata_section:
                    parts = stripped_line.split(',', 1)
                    if len(parts) == 2:
                        key = re.sub(r'[\s-]', '_', parts[0].strip()).lower()
                        metadata[key] = parts[1].strip()
                elif in_data_section:
                    if len(header_lines) < 3: 
                        header_lines.append(stripped_line)
                    else: 
                        data_lines.append(stripped_line)
            
            if len(header_lines) < 3 or not data_lines: 
                continue

            try:
                headers, units, descriptions = header_lines[0].split(','), header_lines[1].split(','), header_lines[2].split(',')
            except IndexError:
                continue

            signal_columns = {}
            for i in range(1, len(headers)):
                header_name = headers[i].strip()
                if header_name and header_name.upper() != 'ABS TIME':
                    signal_name = re.sub(r'[\s\(\)-./]', '_', header_name).lower().strip('_')
                    signal_columns[i] = {
                        'name': signal_name,
                        'unit': units[i].strip() if i < len(units) else '',
                        'description': descriptions[i].strip() if i < len(descriptions) else ''
                    }
            
            # --- Process data lines in chunks to avoid OOM ---
            chunk_size = 10000 # Process 10,000 data lines at a time
            unpivoted_rows_chunk = []

            for line in data_lines:
                values = line.split(',')
                timestamp_str = values[0].strip()
                if not timestamp_str: 
                    continue

                for i, signal_value in enumerate(values[1:], 1):
                    signal_value = signal_value.strip()
                    if signal_value and i in signal_columns:
                        sig_info = signal_columns[i]
                        unpivoted_rows_chunk.append({
                            "timestamp": pd.to_datetime(timestamp_str, format='%d/%m/%Y %H:%M:%S', errors='coerce'),
                            "signal_name": sig_info['name'], "signal_value": signal_value,
                            "signal_unit": sig_info['unit'], "signal_description": sig_info['description'],
                            "imei": metadata.get('imei'), "vin": metadata.get('vin'),
                            "start_time": metadata.get('start_time'), "stop_time": metadata.get('stop_time'),
                            "journey_time": metadata.get('journey_time'), "odo_journey": metadata.get('odo_journey'),
                            "gps_start_pos": metadata.get('gps_start_pos'), "gps_stop_pos": metadata.get('gps_stop_pos'),
                            "file_source": metadata.get('file_source'), "data_section_startrow": metadata.get('data_section_startrow'),
                            "configuration_name": metadata.get('configuration_name'), "logger_serialnumber": metadata.get('logger_serialnumber'),
                            "report_template": metadata.get('report_template'), "source_file": file_name
                        })
                
                # When the chunk is full, yield it as a DataFrame and reset the list
                if len(unpivoted_rows_chunk) >= chunk_size:
                    yield pd.DataFrame(unpivoted_rows_chunk)
                    unpivoted_rows_chunk = []
            
            # Yield any remaining rows after the loop finishes
            if unpivoted_rows_chunk:
                yield pd.DataFrame(unpivoted_rows_chunk)

# COMMAND ----------

# DBTITLE 1,Schema for Parsing Function
parsed_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("signal_name", StringType(), True),
    StructField("signal_value", StringType(), True),
    StructField("signal_unit", StringType(), True),
    StructField("signal_description", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("stop_time", StringType(), True),
    StructField("journey_time", StringType(), True),
    StructField("odo_journey", StringType(), True),
    StructField("gps_start_pos", StringType(), True),
    StructField("gps_stop_pos", StringType(), True),
    StructField("file_source", StringType(), True),
    StructField("data_section_startrow", StringType(), True),
    StructField("configuration_name", StringType(), True),
    StructField("logger_serialnumber", StringType(), True),
    StructField("report_template", StringType(), True),
    StructField("source_file", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Processing Logic for Each Micro-Batch
def process_batch(batch_df, epoch_id):
    """
    This function takes a micro-batch of data (as a static DataFrame),
    parses it, and writes it to the final Delta table.
    """
    print(f"--- Processing batch {epoch_id} ---")

    if batch_df.isEmpty():
        print("Batch is empty, skipping.")
        return

    # Apply the UDF to parse the file content
    parsed_df = batch_df.mapInPandas(parse_and_unpivot_file_udf, schema=parsed_schema)

    # Add the processing timestamp right before writing the data
    parsed_df = parsed_df.withColumn("processing_timestamp", F.current_timestamp())

    (
        parsed_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(BRONZE_PATH)
    )
    
    # Get the number of rows written from the table's transaction history
    delta_table = DeltaTable.forName(spark, BRONZE_PATH)
    last_operation_metrics = delta_table.history(1).select("operationMetrics").collect()[0].operationMetrics
    rows_written = int(last_operation_metrics.get("numOutputRows", 0))

    print(f"--- Write operation for batch {epoch_id} completed. Rows written: {rows_written:,} ---")


# COMMAND ----------

# DBTITLE 1,Define and Run the Autoloader Stream
raw_files_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        # Limit the number of files processed per trigger to avoid OOM errors
        .option("cloudFiles.maxFilesPerTrigger", 4)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv")
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
        .load(VOLUME_PATH)
)

query = (
    raw_files_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
)

query.awaitTermination()

print(f"Successfully processed a batch of new files into {BRONZE_PATH}")
