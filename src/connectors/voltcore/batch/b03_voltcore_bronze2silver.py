# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
import json
import re

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "voltcore_inverters_batch", "Volume name for the API files")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Get Latest Device Metadata from Land Volume
def get_latest_device_metadata(site_id):
    """
    Read the most recent device metadata JSON file for a given site from land volume.
    Files follow pattern: voltcore_devices_site_{site_id}_timestamp_{timestamp}.json
    Returns dict with inverter and battery metadata or None if not found.
    """
    try:
        # List all JSON files for this site
        json_files = []
        for filename in os.listdir(VOLUME_PATH):
            if filename.startswith(f"voltcore_devices_site_{site_id}_") and filename.endswith(".json"):
                # Extract timestamp from filename: voltcore_devices_site_{site_id}_timestamp_{timestamp}.json
                match = re.search(r'timestamp_(\d{8}_\d{6})\.json$', filename)
                if match:
                    timestamp_str = match.group(1)
                    json_files.append((timestamp_str, filename))

        if not json_files:
            print(f"Warning: No device metadata JSON found for site {site_id}")
            return None

        # Sort by timestamp (descending) and get the latest
        json_files.sort(reverse=True)
        latest_file = json_files[0][1]

        # Read the JSON file
        file_path = os.path.join(VOLUME_PATH, latest_file)
        print(f"Reading device metadata from: {latest_file}")

        with open(file_path, 'r') as f:
            data = json.load(f)

        metadata = {"inverter": None, "battery": None}

        # Extract device info
        devices = data.get("records", {}).get("devices", [])
        for device in devices:
            device_name = device.get("name", "")

            # Extract VE.Bus System (inverter) device info
            if device_name == "VE.Bus System":
                metadata["inverter"] = {
                    "product_name": device.get("productName"),
                    "product_code": device.get("productCode"),
                    "firmware_version": device.get("firmwareVersion"),
                    "device_class": device.get("class"),
                    "instance": device.get("instance")
                }

            # Extract Battery Monitor device info
            elif "Battery Monitor" in device_name or "SmartShunt" in device_name or "Lynx Smart BMS" in device_name:
                product_name = device.get("productName", "")
                metadata["battery"] = {
                    "product_name": product_name,
                    "product_code": device.get("productCode"),
                    "firmware_version": device.get("firmwareVersion"),
                    "device_class": device.get("class"),
                    "instance": device.get("instance")
                }

        if not metadata["inverter"]:
            print(f"Warning: No VE.Bus System device found in metadata for site {site_id}")
        if not metadata["battery"]:
            print(f"Warning: No Battery Monitor device found in metadata for site {site_id}")

        return metadata

    except Exception as e:
        print(f"Error reading device metadata for site {site_id}: {e}")
        return None

# COMMAND ----------

# DBTITLE 1,Signal Mappings Per Site
# Site-specific mappings due to different Battery Monitor device IDs and VE.Bus System IDs
SIGNAL_MAPPINGS_PER_SITE = {
    '37151': {  # MOR site
        # Battery Monitor fields - Core metrics
        'state_of_charge': {
            'signal_name': 'State of charge',
            'signal_description': 'Battery Monitor [0]'
        },
        'dc_voltage': {
            'signal_name': 'Voltage',
            'signal_description': 'Battery Monitor [0]'
        },
        'dc_current': {
            'signal_name': 'Current',
            'signal_description': 'Battery Monitor [0]'
        },
        # Battery Monitor fields - Min/Max voltages (runtime telemetry)
        'dc_voltage_max': {
            'signal_name': 'Maximum voltage',
            'signal_description': 'Battery Monitor [0]'
        },
        'dc_voltage_min': {
            'signal_name': 'Minimum voltage',
            'signal_description': 'Battery Monitor [0]'
        },
        # Battery Monitor fields - Lifecycle metrics
        'charge_cycles': {
            'signal_name': 'Charge cycles',
            'signal_description': 'Battery Monitor [0]'
        },
        # Battery Monitor fields - Energy metrics
        'energy_charge': {
            'signal_name': 'Charged Energy',
            'signal_description': 'Battery Monitor [0]'
        },
        'energy_discharge': {
            'signal_name': 'Discharged Energy',
            'signal_description': 'Battery Monitor [0]'
        },
        # Battery Monitor fields - State and status
        'battery_state': {
            'signal_name': 'State',
            'signal_description': 'Battery Monitor [0]'
        },
        # VE.Bus System AC measurements (Inverter)
        'input_voltage': {
            'signal_name': 'Input voltage phase 1',
            'signal_description': 'VE.Bus System [0]'
        },
        'input_current': {
            'signal_name': 'Input current phase 1',
            'signal_description': 'VE.Bus System [0]'
        },
        'input_frequency': {
            'signal_name': 'Input frequency 1',
            'signal_description': 'VE.Bus System [0]'
        },
        'input_power': {
            'signal_name': 'Input power 1',
            'signal_description': 'VE.Bus System [0]'
        },
        'ac_voltage_l1': {
            'signal_name': 'Output voltage phase 1',
            'signal_description': 'VE.Bus System [0]'
        },
        'ac_current_l1': {
            'signal_name': 'Output current phase 1',
            'signal_description': 'VE.Bus System [0]'
        },
        'ac_frequency': {
            'signal_name': 'Output frequency',
            'signal_description': 'VE.Bus System [0]'
        },
        'ac_power': {
            'signal_name': 'Output power 1',
            'signal_description': 'VE.Bus System [0]'
        },
        'charge_state': {
            'signal_name': 'Charge state',
            'signal_description': 'VE.Bus System [0]'
        }
    },
    '407966': {  # TU Delft site
        # Battery Monitor fields - Core metrics
        'state_of_charge': {
            'signal_name': 'State of charge',
            'signal_description': 'Battery Monitor [279]'
        },
        'dc_voltage': {
            'signal_name': 'Voltage',
            'signal_description': 'Battery Monitor [279]'
        },
        'dc_current': {
            'signal_name': 'Current',
            'signal_description': 'Battery Monitor [279]'
        },
        # Battery Monitor fields - Min/Max voltages (runtime telemetry)
        'dc_voltage_max': {
            'signal_name': 'Maximum voltage',
            'signal_description': 'Battery Monitor [279]'
        },
        'dc_voltage_min': {
            'signal_name': 'Minimum voltage',
            'signal_description': 'Battery Monitor [279]'
        },
        # Battery Monitor fields - Lifecycle metrics
        'charge_cycles': {
            'signal_name': 'Charge cycles',
            'signal_description': 'Battery Monitor [279]'
        },
        # Battery Monitor fields - Energy metrics
        'energy_charge': {
            'signal_name': 'Charged Energy',
            'signal_description': 'Battery Monitor [279]'
        },
        'energy_discharge': {
            'signal_name': 'Discharged Energy',
            'signal_description': 'Battery Monitor [279]'
        },
        # Battery Monitor fields - State and status
        'battery_state': {
            'signal_name': 'State',
            'signal_description': 'Battery Monitor [279]'
        },
        # Temperature sensors
        'temperature_cabinet': {
            'signal_name': 'Temperature',
            'signal_description': 'Temperature sensor [20]'
        },
        'temperature_ambient': {
            'signal_name': 'Temperature',
            'signal_description': 'Temperature sensor [21]'
        },
        # VE.Bus System AC measurements (Inverter)
        'input_voltage': {
            'signal_name': 'Input voltage phase 1',
            'signal_description': 'VE.Bus System [276]'
        },
        'input_current': {
            'signal_name': 'Input current phase 1',
            'signal_description': 'VE.Bus System [276]'
        },
        'input_frequency': {
            'signal_name': 'Input frequency 1',
            'signal_description': 'VE.Bus System [276]'
        },
        'input_power': {
            'signal_name': 'Input power 1',
            'signal_description': 'VE.Bus System [276]'
        },
        'ac_voltage_l1': {
            'signal_name': 'Output voltage phase 1',
            'signal_description': 'VE.Bus System [276]'
        },
        'ac_current_l1': {
            'signal_name': 'Output current phase 1',
            'signal_description': 'VE.Bus System [276]'
        },
        'ac_frequency': {
            'signal_name': 'Output frequency',
            'signal_description': 'VE.Bus System [276]'
        },
        'ac_power': {
            'signal_name': 'Output power 1',
            'signal_description': 'VE.Bus System [276]'
        },
        'charge_state': {
            'signal_name': 'Charge state',
            'signal_description': 'VE.Bus System [276]'
        }
    }
}

# COMMAND ----------

# DBTITLE 1,Discover Bronze Tables for Voltcore Devices
# Get all bronze tables that match voltcore_{product_code}_batch pattern
all_tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.bronze").collect()
voltcore_bronze_tables = [
    row.tableName for row in all_tables
    if row.tableName.startswith('voltcore_') and row.tableName.endswith('_batch')
]

if not voltcore_bronze_tables:
    raise ValueError("No voltcore_*_batch tables found in bronze layer")

print(f"Found {len(voltcore_bronze_tables)} Voltcore bronze tables:")
for table in voltcore_bronze_tables:
    print(f"  - {table}")

# Extract product_code from table names and query site_id from bronze tables
import re
site_product_combinations = []
for table in voltcore_bronze_tables:
    # Extract product_code from voltcore_{product_code}_batch
    match = re.search(r'voltcore_([A-Za-z0-9]+)_batch', table)
    if match:
        product_code = match.group(1)
        # Query the bronze table to get the site_id
        bronze_table = f"{CATALOG_NAME}.bronze.{table}"
        site_id_result = spark.sql(f"SELECT DISTINCT id_site FROM {bronze_table} LIMIT 1").collect()
        if site_id_result:
            site_id = site_id_result[0].id_site
            site_product_combinations.append({
                'site_id': site_id,
                'product_code': product_code
            })

print(f"\nSite/Product combinations: {len(site_product_combinations)}")
for combo in site_product_combinations:
    print(f"  - Site {combo['site_id']}, Product {combo['product_code']}")

# COMMAND ----------

# DBTITLE 1,Build Pivot Query Per Site
def build_pivot_query_for_site(site_id, product_code, device_metadata):
    """
    Build SQL query to pivot bronze data for a specific site and product_code.
    Enriches with device metadata from JSON files instead of querying gold table.

    Args:
        site_id: Voltcore site ID
        product_code: Device product code
        device_metadata: Dict containing device metadata from JSON file
    """

    bronze_table = f"{CATALOG_NAME}.bronze.voltcore_{product_code}_batch"
    mappings = SIGNAL_MAPPINGS_PER_SITE.get(site_id, {})

    if not mappings:
        print(f"Warning: No signal mappings defined for site {site_id}, skipping")
        return None

    # Build the CASE statements for each metric
    # Use Haystack-compliant id_site format to match dimension tables
    select_parts = ["b.timestamp", f"'voltcore-{site_id}' AS id_site"]

    # Battery Monitor fields
    # SOC
    if 'state_of_charge' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['state_of_charge']['signal_name']}'
                AND signal_description = '{mappings['state_of_charge']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS state_of_charge
        """)

    # Voltage
    if 'dc_voltage' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['dc_voltage']['signal_name']}'
                AND signal_description = '{mappings['dc_voltage']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS dc_voltage
        """)

    # Current
    if 'dc_current' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['dc_current']['signal_name']}'
                AND signal_description = '{mappings['dc_current']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS dc_current
        """)

    # Battery Monitor - Min/Max Voltages
    if 'dc_voltage_max' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['dc_voltage_max']['signal_name']}'
                AND signal_description = '{mappings['dc_voltage_max']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS dc_voltage_max
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS dc_voltage_max")

    if 'dc_voltage_min' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['dc_voltage_min']['signal_name']}'
                AND signal_description = '{mappings['dc_voltage_min']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS dc_voltage_min
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS dc_voltage_min")

    # Battery Monitor - Lifecycle Metrics
    if 'charge_cycles' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['charge_cycles']['signal_name']}'
                AND signal_description = '{mappings['charge_cycles']['signal_description']}'
                THEN CAST(CAST(signal_value AS DECIMAL(10,2)) AS INT)
            END) AS charge_cycles
        """)
    else:
        select_parts.append("CAST(NULL AS INT) AS charge_cycles")

    # Battery Monitor - Energy Metrics
    if 'energy_charge' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['energy_charge']['signal_name']}'
                AND signal_description = '{mappings['energy_charge']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS energy_charge
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS energy_charge")

    if 'energy_discharge' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['energy_discharge']['signal_name']}'
                AND signal_description = '{mappings['energy_discharge']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS energy_discharge
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS energy_discharge")

    # Battery Monitor - State
    if 'battery_state' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['battery_state']['signal_name']}'
                AND signal_description = '{mappings['battery_state']['signal_description']}'
                THEN signal_value
            END) AS battery_state
        """)
    else:
        select_parts.append("CAST(NULL AS STRING) AS battery_state")

    # Temperature sensors (optional)
    # Inside Temperature
    if 'temperature_cabinet' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['temperature_cabinet']['signal_name']}'
                AND signal_description = '{mappings['temperature_cabinet']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS temperature_cabinet
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS temperature_cabinet")

    # Outside Temperature
    if 'temperature_ambient' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['temperature_ambient']['signal_name']}'
                AND signal_description = '{mappings['temperature_ambient']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS temperature_ambient
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS temperature_ambient")

    # VE.Bus System AC measurements (Inverter)
    # Input Voltage
    if 'input_voltage' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['input_voltage']['signal_name']}'
                AND signal_description = '{mappings['input_voltage']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS input_voltage
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS input_voltage")

    # Input Current
    if 'input_current' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['input_current']['signal_name']}'
                AND signal_description = '{mappings['input_current']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS input_current
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS input_current")

    # Input Frequency
    if 'input_frequency' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['input_frequency']['signal_name']}'
                AND signal_description = '{mappings['input_frequency']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS input_frequency
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS input_frequency")

    # Input Power
    if 'input_power' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['input_power']['signal_name']}'
                AND signal_description = '{mappings['input_power']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS input_power
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS input_power")

    # Output Voltage
    if 'ac_voltage_l1' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['ac_voltage_l1']['signal_name']}'
                AND signal_description = '{mappings['ac_voltage_l1']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS ac_voltage_l1
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS ac_voltage_l1")

    # Output Current
    if 'ac_current_l1' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['ac_current_l1']['signal_name']}'
                AND signal_description = '{mappings['ac_current_l1']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS ac_current_l1
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS ac_current_l1")

    # Output Frequency
    if 'ac_frequency' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['ac_frequency']['signal_name']}'
                AND signal_description = '{mappings['ac_frequency']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS ac_frequency
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS ac_frequency")

    # Output Power
    if 'ac_power' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['ac_power']['signal_name']}'
                AND signal_description = '{mappings['ac_power']['signal_description']}'
                THEN CAST(signal_value AS DECIMAL(10,2))
            END) AS ac_power
        """)
    else:
        select_parts.append("CAST(NULL AS DECIMAL(10,2)) AS ac_power")

    # Charge State (Inverter State)
    if 'charge_state' in mappings:
        select_parts.append(f"""
            MAX(CASE
                WHEN signal_name = '{mappings['charge_state']['signal_name']}'
                AND signal_description = '{mappings['charge_state']['signal_description']}'
                THEN signal_value
            END) AS charge_state
        """)
    else:
        select_parts.append("CAST(NULL AS STRING) AS charge_state")

    # Get metadata values
    if not device_metadata:
        print(f"Warning: No device metadata available for site {site_id}")
        inverter_product_name = "Unknown"
        battery_product_name = "Unknown"
    else:
        inverter_metadata = device_metadata.get("inverter", {})
        battery_metadata = device_metadata.get("battery", {})
        inverter_product_name = inverter_metadata.get("product_name", "Unknown") if inverter_metadata else "Unknown"
        battery_product_name = battery_metadata.get("product_name", "Unknown") if battery_metadata else "Unknown"

    # Extract inverter model series and SKU from product_name
    # Example: "MultiPlus-II 48/5000/70-50" -> series: "MultiPlus-II", sku: "48/5000/70-50"
    inverter_parts = inverter_product_name.split(' ', 1)
    inverter_model_series = inverter_parts[0] if inverter_parts else inverter_product_name
    inverter_model_sku = inverter_parts[1] if len(inverter_parts) > 1 else ""

    # Extract battery model series and SKU from product_name
    # Example: "SmartShunt 500A" -> series: "SmartShunt", sku: "500A"
    battery_parts = battery_product_name.split(' ', 1) if battery_product_name != "Unknown" else ["Unknown", ""]
    battery_model_series = battery_parts[0] if battery_parts else battery_product_name
    battery_model_sku = battery_parts[1] if len(battery_parts) > 1 else ""

    # Build the query with metadata from JSON file
    query = f"""
        WITH pivoted AS (
            SELECT {', '.join(select_parts)}
            FROM {bronze_table} b
            WHERE b.timestamp IS NOT NULL
            GROUP BY b.timestamp
            HAVING COALESCE(state_of_charge, dc_voltage, dc_current, dc_voltage_max, dc_voltage_min,
                           charge_cycles, energy_charge, energy_discharge, battery_state,
                           temperature_cabinet, temperature_ambient,
                           input_voltage, input_current, input_frequency, input_power,
                           ac_voltage_l1, ac_current_l1, ac_frequency, ac_power,
                           charge_state) IS NOT NULL
        )
        SELECT
            p.*,
            'Voltcore' AS model,
            '{inverter_model_series}' AS model_series,
            '{inverter_model_sku}' AS model_sku,
            MD5(CONCAT('Voltcore', '{inverter_model_series}', '{inverter_model_sku}')) AS sk_inverter,
            '{battery_model_series}' AS battery_model_series,
            '{battery_model_sku}' AS battery_model_sku,
            MD5(CONCAT('Voltcore', '{battery_model_series}', '{battery_model_sku}')) AS sk_battery
        FROM pivoted p
    """

    return query

# COMMAND ----------

# DBTITLE 1,Create Silver Tables Per Device
for combo in site_product_combinations:
    site_id = combo['site_id']
    product_code = combo['product_code']
    silver_table = f"{CATALOG_NAME}.silver.voltcore_{product_code}_batch"

    # Check if table exists
    table_exists = spark.catalog.tableExists(silver_table)

    if not table_exists:
        print(f"\nCreating silver table: {silver_table}")

        spark.sql(f"""
        CREATE TABLE {silver_table} (
            timestamp TIMESTAMP NOT NULL COMMENT 'Signal measurement time',
            id_site STRING NOT NULL COMMENT 'Voltcore installation site ID (Site {site_id}, Product {product_code})',

            -- Battery Monitor fields - Core metrics
            state_of_charge DECIMAL(10,2) COMMENT 'State of charge percentage (Haystack: SoC)',
            dc_voltage DECIMAL(10,2) COMMENT 'Battery dc_voltage in volts (Haystack: V)',
            dc_current DECIMAL(10,2) COMMENT 'Battery dc_current in amperes (Haystack: A)',

            -- Battery Monitor fields - Min/Max voltages (runtime telemetry)
            dc_voltage_max DECIMAL(10,2) COMMENT 'Maximum battery dc_voltage recorded in volts (Haystack: VMax)',
            dc_voltage_min DECIMAL(10,2) COMMENT 'Minimum battery dc_voltage recorded in volts (Haystack: VMin)',

            -- Battery Monitor fields - Lifecycle metrics
            charge_cycles INT COMMENT 'Number of charge cycles executed (Haystack: NCyc)',

            -- Battery Monitor fields - Energy metrics
            energy_charge DECIMAL(10,2) COMMENT 'Total energy charged in kWh',
            energy_discharge DECIMAL(10,2) COMMENT 'Total energy discharged in kWh',

            -- Battery Monitor fields - State
            battery_state STRING COMMENT 'Battery bank state (Haystack: State) - e.g., Running, Standby, Fault',

            -- Temperature sensor fields
            temperature_cabinet DECIMAL(10,2) COMMENT 'Inside temperature in celsius',
            temperature_ambient DECIMAL(10,2) COMMENT 'Outside temperature in celsius',

            -- VE.Bus System AC measurements (Inverter)
            input_voltage DECIMAL(10,2) COMMENT 'AC input dc_voltage phase 1 in volts',
            input_current DECIMAL(10,2) COMMENT 'AC input dc_current phase 1 in amperes',
            input_frequency DECIMAL(10,2) COMMENT 'AC input frequency in hertz',
            input_power DECIMAL(10,2) COMMENT 'AC input power phase 1 in watts',
            ac_voltage_l1 DECIMAL(10,2) COMMENT 'AC output dc_voltage phase 1 in volts',
            ac_current_l1 DECIMAL(10,2) COMMENT 'AC output dc_current phase 1 in amperes',
            ac_frequency DECIMAL(10,2) COMMENT 'AC output frequency in hertz',
            ac_power DECIMAL(10,2) COMMENT 'AC output power phase 1 in watts',
            charge_state STRING COMMENT 'Inverter charge state (Off, Bulk, Absorption, Float, Storage, Inverting)',

            -- Inverter Metadata
            model STRING COMMENT 'Device brand/manufacturer (Voltcore)',
            model_series STRING COMMENT 'Inverter model series (e.g., MultiPlus-II)',
            model_sku STRING COMMENT 'Inverter model SKU (e.g., 24/3000/70-32)',
            sk_inverter STRING COMMENT 'Device surrogate key - references d_inverters.sk_inverter',

            -- Battery Metadata
            battery_model_series STRING COMMENT 'Battery monitor model series (e.g., SmartShunt, Lynx)',
            battery_model_sku STRING COMMENT 'Battery monitor model SKU (e.g., 500A, 1000A)',
            sk_battery STRING COMMENT 'Device surrogate key - references d_batteries.sk_battery'
        )
        CLUSTER BY AUTO
        COMMENT 'Standardized time-series telemetry data from Voltcore devices (Site {site_id}, Product {product_code}) with combined Battery Monitor and VE.Bus System signals, aligned with Haystack ontology.'
        """)

        print(f"Created table: {silver_table}")
    else:
        print(f"\nTable already exists: {silver_table}")

# COMMAND ----------

# DBTITLE 1,Process Each Device
for combo in site_product_combinations:
    site_id = combo['site_id']
    product_code = combo['product_code']
    print(f"\n{'='*60}")
    print(f"Processing Site: {site_id} (Product: {product_code})")
    print(f"{'='*60}")

    silver_table = f"{CATALOG_NAME}.silver.voltcore_{product_code}_batch"

    # Get latest device metadata from land volume JSON files
    device_metadata = get_latest_device_metadata(site_id)

    # Build pivot query with device metadata
    pivot_query = build_pivot_query_for_site(site_id, product_code, device_metadata)

    if not pivot_query:
        print(f"Skipping site {site_id} - no mappings defined")
        continue

    # Get max timestamp from existing silver table
    max_ts_result = spark.sql(f"SELECT MAX(timestamp) as max_ts FROM {silver_table}").collect()[0]
    max_ts = max_ts_result.max_ts if max_ts_result.max_ts else None

    if max_ts:
        print(f"Latest timestamp in silver: {max_ts}")

        # Build incremental query
        incremental_query = f"""
        WITH pivoted_data AS ({pivot_query})
        SELECT *
        FROM pivoted_data
        WHERE timestamp > CAST('{max_ts}' AS TIMESTAMP)
        """
    else:
        print("No existing data - performing initial load")
        incremental_query = pivot_query

    # Execute query
    new_data_df = spark.sql(incremental_query)
    new_rows = new_data_df.count()

    if new_rows > 0:
        # Append new data
        new_data_df.write.mode("append").saveAsTable(silver_table)
        print(f"✓ Appended {new_rows:,} new rows to {silver_table}")
    else:
        print(f"✓ No new data to append for site {site_id}")
