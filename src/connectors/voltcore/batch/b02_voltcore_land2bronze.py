# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "voltcore_inverters_batch", "Volume Name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}/"

CHECKPOINT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/"

# COMMAND ----------

# DBTITLE 1,Create Bronze Table Function
def create_bronze_table(catalog_name, site_id, product_code):
    """Create bronze table for a specific site_id and product_code if it doesn't exist."""
    table_name = f"voltcore_{product_code}_batch"
    table_path = f"{catalog_name}.bronze.{table_name}"

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
      timestamp TIMESTAMP COMMENT 'Signal measurement time',
      signal_name STRING COMMENT 'Signal identifier',
      signal_description STRING COMMENT 'Signal description',
      signal_unit STRING COMMENT 'Signal unit of measurement',
      signal_value STRING COMMENT 'Measured value',
      id_site STRING COMMENT 'Voltcore installation site ID (Site {site_id}, Product {product_code})',
      source_file STRING COMMENT 'Source CSV file in the landing volume for lineage',
      bronze_processing_timestamp TIMESTAMP COMMENT 'When the record was processed in the bronze layer'
    )
    CLUSTER BY AUTO
    COMMENT 'Stores unpivoted time-series signal data for Voltcore device (Site {site_id}, Product {product_code} - MultiPlus-II).'
    """)

    print(f"Created/verified bronze table: {table_path}")
    return table_path

# COMMAND ----------

# DBTITLE 1,Processing Function per Site
def process_site_batch(batch_df, batch_id, site_id, product_code):
    """
    Process batch of CSV files for a specific site_id and product_code.
    Reads CSV content, parses 3 header rows, unpivots to narrow format.
    """
    # Filter files for this site_id
    site_files = batch_df.filter(F.col("site_id") == site_id).select("_metadata.file_path").distinct().collect()

    if not site_files:
        return

    print(f"  Site {site_id}: Processing {len(site_files)} file(s)")

    all_rows = []

    for file_row in site_files:
        file_path = file_row['file_path']
        filename = os.path.basename(file_path)

        print(f"    Processing {filename}")

        try:
            # Read the CSV file content
            with open(file_path.replace('dbfs:', '/dbfs'), 'r') as f:
                lines = f.readlines()

            if len(lines) < 4:
                print("      Warning: File has fewer than 4 lines, skipping")
                continue

            # Parse the 3 header rows
            header_row_1 = lines[0].replace('"', '').strip().split(',')  # signal_description
            header_row_2 = lines[1].replace('"', '').strip().split(',')  # signal_name
            header_row_3 = lines[2].replace('"', '').strip().split(',')  # signal_unit

            # Process data rows (starting from row 4)
            data_lines = lines[3:]

            for line in data_lines:
                if not line.strip():
                    continue

                values = line.replace('"', '').strip().split(',')

                # First value is always timestamp
                timestamp = values[0] if values else None

                if not timestamp:
                    continue

                # Process each signal (skip timestamp column at index 0)
                for i in range(1, len(values)):
                    signal_value = values[i].strip() if i < len(values) else ""

                    # Only add if there's a value
                    if signal_value:
                        # Get metadata from header rows
                        signal_description = header_row_1[i].strip() if i < len(header_row_1) else ""
                        signal_name = header_row_2[i].strip() if i < len(header_row_2) else ""
                        signal_unit = header_row_3[i].strip() if i < len(header_row_3) else ""

                        all_rows.append((
                            timestamp,
                            signal_name,
                            signal_description,
                            signal_unit,
                            signal_value,
                            site_id,
                            filename
                        ))

        except Exception as e:
            print(f"      Error processing {filename}: {e}")
            continue

    # Create DataFrame from collected rows
    if all_rows:
        schema = StructType([
            StructField("timestamp_str", StringType(), True),
            StructField("signal_name", StringType(), True),
            StructField("signal_description", StringType(), True),
            StructField("signal_unit", StringType(), True),
            StructField("signal_value", StringType(), True),
            StructField("id_site", StringType(), True),
            StructField("source_file", StringType(), True)
        ])

        df = spark.createDataFrame(all_rows, schema)

        # Convert timestamp and add processing timestamp
        df = (
            df
            .withColumn("timestamp", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("bronze_processing_timestamp", F.current_timestamp())
            .drop("timestamp_str")
            .select(
                "timestamp",
                "signal_name",
                "signal_description",
                "signal_unit",
                "signal_value",
                "id_site",
                "source_file",
                "bronze_processing_timestamp"
            )
        )

        # Write to product-specific bronze table
        table_path = f"{CATALOG_NAME}.bronze.voltcore_{product_code}_batch"
        df.write.mode("append").saveAsTable(table_path)

        print(f"    Site {site_id} (Product {product_code}): Wrote {len(all_rows)} rows to {table_path}")

# COMMAND ----------

# DBTITLE 1,Main Processing Function
def process_voltcore_batch(batch_df, batch_id):
    """
    Process each batch of CSV files from Autoloader.
    Extracts site_id and product_code from filename, groups by site_id.

    Expected filename pattern: voltcore_site_{site_id}_device_{product_code}_timestamp_{timestamp}.csv
    """
    # Add metadata columns extracted from filename
    # Expected pattern: voltcore_site_{site_id}_device_{product_code}_timestamp_{timestamp}.csv
    batch_with_metadata = batch_df.withColumn(
        "filename",
        F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1)
    ).withColumn(
        "site_id",
        F.regexp_extract(F.col("filename"), r"voltcore_site_(\d+)_device_[A-Za-z0-9]+_timestamp_\d{8}_\d{6}\.csv", 1)
    ).withColumn(
        "product_code",
        F.regexp_extract(F.col("filename"), r"voltcore_site_\d+_device_([A-Za-z0-9]+)_timestamp_\d{8}_\d{6}\.csv", 1)
    )

    # Get unique site_id + product_code combinations in this batch
    site_product_combinations = [
        {"site_id": row['site_id'], "product_code": row['product_code']}
        for row in batch_with_metadata.select("site_id", "product_code").distinct().collect()
        if row['site_id'] and row['product_code']
    ]

    if not site_product_combinations:
        print(f"Batch {batch_id}: No valid site_id/product_code combinations found")
        return

    print(f"Batch {batch_id}: Found {len(site_product_combinations)} site/product combination(s)")
    for combo in site_product_combinations:
        print(f"  - Site {combo['site_id']}, Product {combo['product_code']}")

    # Create bronze tables for each site_id + product_code combination
    for combo in site_product_combinations:
        create_bronze_table(CATALOG_NAME, combo['site_id'], combo['product_code'])

    # Process each site separately
    for combo in site_product_combinations:
        process_site_batch(batch_with_metadata, batch_id, combo['site_id'], combo['product_code'])

    print(f"Batch {batch_id}: Processing complete")

# COMMAND ----------

# DBTITLE 1,Read CSV Files with Autoloader
# Read as binary to get file paths, then process each file individually
stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE_PATH}voltcore_raw_batch/")
        .option("pathGlobFilter", "*.csv")
        .load(VOLUME_PATH)
)

# COMMAND ----------

# DBTITLE 1,Process Stream with foreachBatch
query = (
    stream_df
    .writeStream
    .foreachBatch(process_voltcore_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}voltcore_raw_batch/")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# DBTITLE 1,Wait for Stream to Complete
query.awaitTermination()
