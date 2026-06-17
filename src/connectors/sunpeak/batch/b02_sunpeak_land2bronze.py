# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import os
import json

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "sunpeak_inverters_batch", "Volume Name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}/"

CHECKPOINT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/"

# COMMAND ----------

# DBTITLE 1,Create Bronze Table for Inverter Technical Data
def create_bronze_table_inverter(catalog_name, site_id, serial_number):
    """Create bronze table for Sunpeak inverter technical data for a specific site and inverter."""
    # Sanitize serial number for table name (replace special characters with underscores, lowercase)
    sanitized_sn = serial_number.replace('-', '_').replace(' ', '_').lower()
    table_name = f"sunpeak_{sanitized_sn}_batch"
    table_path = f"{catalog_name}.bronze.{table_name}"

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
      timestamp TIMESTAMP COMMENT 'Telemetry measurement time',
      id_site STRING COMMENT 'Sunpeak site ID (Site {site_id}, SN {serial_number})',
      serial_number STRING COMMENT 'Inverter serial number',
      total_active_power STRING COMMENT 'Total active power in Watts',
      dc_voltage STRING COMMENT 'DC voltage in Volts',
      ground_fault_resistance STRING COMMENT 'Ground fault resistance in Ohms',
      power_limit STRING COMMENT 'Power limit percentage',
      total_energy STRING COMMENT 'Total lifetime energy in Watt-hours',
      temperature STRING COMMENT 'Inverter temperature in Celsius',
      inverter_mode STRING COMMENT 'Inverter operating mode (e.g., MPPT, OFF, SLEEPING)',
      operation_mode STRING COMMENT 'Operation mode: 0=On-grid, 1=Off-grid PV/battery, 2=Off-grid with generator',
      l1_ac_current STRING COMMENT 'L1 AC current in Amperes',
      l1_ac_voltage STRING COMMENT 'L1 AC voltage in Volts',
      l1_ac_frequency STRING COMMENT 'L1 AC frequency in Hertz',
      l1_apparent_power STRING COMMENT 'L1 apparent power in VA',
      l1_active_power STRING COMMENT 'L1 active power in Watts',
      l1_reactive_power STRING COMMENT 'L1 reactive power in VAR',
      l1_cos_phi STRING COMMENT 'L1 power factor (cos phi)',
      source_file STRING COMMENT 'Source JSON file in the landing volume for lineage',
      bronze_processing_timestamp TIMESTAMP COMMENT 'When the record was processed in the bronze layer'
    )
    CLUSTER BY AUTO
    COMMENT 'Stores Sunpeak inverter technical data for device (Site {site_id}, SN {serial_number}).'
    """)

    print(f"Created/verified bronze inverter table: {table_path}")
    return table_path

# COMMAND ----------

# DBTITLE 1,Processing Function for Inverter Technical Data
def process_inverter_batch(batch_df, batch_id, site_id, serial_number):
    """
    Process batch of inverter technical data JSON files for a specific site_id and serial number.
    Extracts detailed telemetry data from the 'data.telemetries' structure.
    """
    # Filter files for this site_id and serial number
    site_files = batch_df.filter(
        (F.col("site_id") == site_id) &
        (F.col("serial_number") == serial_number)
    ).select("_metadata.file_path").distinct().collect()

    if not site_files:
        return

    print(f"  Site {site_id} Inverter {serial_number}: Processing {len(site_files)} file(s)")

    all_rows = []

    for file_row in site_files:
        file_path = file_row['file_path']
        filename = os.path.basename(file_path)

        print(f"    Processing {filename}")

        try:
            # Read the JSON file content
            with open(file_path.replace('dbfs:', '/dbfs'), 'r') as f:
                data = json.load(f)

            # Extract telemetry data structure
            data_section = data.get('data', {})
            telemetries = data_section.get('telemetries', [])

            # Process each telemetry record
            for telemetry in telemetries:
                timestamp = telemetry.get('date')
                total_active_power = telemetry.get('totalActivePower')
                dc_voltage = telemetry.get('dcVoltage')
                ground_fault_resistance = telemetry.get('groundFaultResistance')
                power_limit = telemetry.get('powerLimit')
                total_energy = telemetry.get('totalEnergy')
                temperature = telemetry.get('temperature')
                inverter_mode = telemetry.get('inverterMode')
                operation_mode = telemetry.get('operationMode')

                # Extract L1 data (for single-phase inverters)
                l1_data = telemetry.get('L1Data', {})
                l1_ac_current = l1_data.get('acCurrent')
                l1_ac_voltage = l1_data.get('acVoltage')
                l1_ac_frequency = l1_data.get('acFrequency')
                l1_apparent_power = l1_data.get('apparentPower')
                l1_active_power = l1_data.get('activePower')
                l1_reactive_power = l1_data.get('reactivePower')
                l1_cos_phi = l1_data.get('cosPhi')

                # Convert all values to strings
                # Use Haystack-compliant id_site format to match dimension tables
                all_rows.append((
                    timestamp,
                    f"sunpeak-{site_id}",
                    serial_number,
                    str(total_active_power) if total_active_power is not None else None,
                    str(dc_voltage) if dc_voltage is not None else None,
                    str(ground_fault_resistance) if ground_fault_resistance is not None else None,
                    str(power_limit) if power_limit is not None else None,
                    str(total_energy) if total_energy is not None else None,
                    str(temperature) if temperature is not None else None,
                    inverter_mode,
                    str(operation_mode) if operation_mode is not None else None,
                    str(l1_ac_current) if l1_ac_current is not None else None,
                    str(l1_ac_voltage) if l1_ac_voltage is not None else None,
                    str(l1_ac_frequency) if l1_ac_frequency is not None else None,
                    str(l1_apparent_power) if l1_apparent_power is not None else None,
                    str(l1_active_power) if l1_active_power is not None else None,
                    str(l1_reactive_power) if l1_reactive_power is not None else None,
                    str(l1_cos_phi) if l1_cos_phi is not None else None,
                    filename
                ))

        except Exception as e:
            print(f"      Error processing {filename}: {e}")
            continue

    # Create DataFrame from collected rows
    if all_rows:
        schema = StructType([
            StructField("timestamp_str", StringType(), True),
            StructField("id_site", StringType(), True),
            StructField("serial_number", StringType(), True),
            StructField("total_active_power", StringType(), True),
            StructField("dc_voltage", StringType(), True),
            StructField("ground_fault_resistance", StringType(), True),
            StructField("power_limit", StringType(), True),
            StructField("total_energy", StringType(), True),
            StructField("temperature", StringType(), True),
            StructField("inverter_mode", StringType(), True),
            StructField("operation_mode", StringType(), True),
            StructField("l1_ac_current", StringType(), True),
            StructField("l1_ac_voltage", StringType(), True),
            StructField("l1_ac_frequency", StringType(), True),
            StructField("l1_apparent_power", StringType(), True),
            StructField("l1_active_power", StringType(), True),
            StructField("l1_reactive_power", StringType(), True),
            StructField("l1_cos_phi", StringType(), True),
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
                "id_site",
                "serial_number",
                "total_active_power",
                "dc_voltage",
                "ground_fault_resistance",
                "power_limit",
                "total_energy",
                "temperature",
                "inverter_mode",
                "operation_mode",
                "l1_ac_current",
                "l1_ac_voltage",
                "l1_ac_frequency",
                "l1_apparent_power",
                "l1_active_power",
                "l1_reactive_power",
                "l1_cos_phi",
                "source_file",
                "bronze_processing_timestamp"
            )
        )

        # Write to device-specific bronze table
        sanitized_sn = serial_number.replace('-', '_').replace(' ', '_').lower()
        table_path = f"{CATALOG_NAME}.bronze.sunpeak_{sanitized_sn}_batch"
        df.write.mode("append").saveAsTable(table_path)

        print(f"    Site {site_id} Inverter {serial_number}: Wrote {len(all_rows)} rows to {table_path}")

# COMMAND ----------

# DBTITLE 1,Main Processing Function
def process_sunpeak_batch(batch_df, batch_id):
    """
    Process each batch of inverter JSON files from Autoloader.
    Extracts site_id and serial_number from filename.

    Expected filename patterns:
    - Regular batch: sunpeak_inverter_{site_id}_{serial_number}_{timestamp}.json
    - Backfill: sunpeak_inverter_{site_id}_{serial_number}_{timestamp}_backfill_{date}.json
    """
    # Add metadata columns extracted from filename
    batch_with_metadata = batch_df.withColumn(
        "filename",
        F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1)
    ).withColumn(
        # Extract site_id: matches digits after "sunpeak_inverter_"
        "site_id",
        F.regexp_extract(F.col("filename"), r"sunpeak_inverter_(\d+)_", 1)
    ).withColumn(
        # Extract serial_number: matches alphanumeric+hyphens between site_id and timestamp
        "serial_number",
        F.regexp_extract(F.col("filename"), r"sunpeak_inverter_\d+_([A-Za-z0-9\-_]+)_\d{8}_\d{6}", 1)
    )

    # Get unique site_id + serial_number combinations in this batch
    site_inverter_combinations = [
        {"site_id": row['site_id'], "serial_number": row['serial_number']}
        for row in batch_with_metadata.select("site_id", "serial_number").distinct().collect()
        if row['site_id'] and row['serial_number']
    ]

    if not site_inverter_combinations:
        print(f"Batch {batch_id}: No valid site_id/serial_number combinations found")
        return

    print(f"Batch {batch_id}: Found {len(site_inverter_combinations)} inverter(s)")
    for combo in site_inverter_combinations:
        print(f"  - Site {combo['site_id']}, Serial {combo['serial_number']}")

    # Create bronze tables for each site_id and serial number
    for combo in site_inverter_combinations:
        create_bronze_table_inverter(CATALOG_NAME, combo['site_id'], combo['serial_number'])

    # Process each inverter
    for combo in site_inverter_combinations:
        process_inverter_batch(batch_with_metadata, batch_id, combo['site_id'], combo['serial_number'])

    print(f"Batch {batch_id}: Processing complete")

# COMMAND ----------

# DBTITLE 1,Read JSON Files with Autoloader
# Read as binary to get file paths, then process each file individually
stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE_PATH}sunpeak_raw_batch/")
        .option("pathGlobFilter", "*.json")
        .load(VOLUME_PATH)
)

# COMMAND ----------

# DBTITLE 1,Process Stream with foreachBatch
query = (
    stream_df
    .writeStream
    .foreachBatch(process_sunpeak_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}sunpeak_raw_batch/")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# DBTITLE 1,Wait for Stream to Complete
query.awaitTermination()
