# Databricks notebook source
# DBTITLE 1,Imports and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# DBTITLE 1,Site to Serial Number Mapping
# Mapping of site_id to serial_number (inverter identifier)
SITE_TO_SERIAL_NUMBER = {
    '58604': '7f181dc8_7c'  # Sunpeak inverter serial number
}

# Reverse mapping for discovery
SERIAL_NUMBER_TO_SITE = {v: k for k, v in SITE_TO_SERIAL_NUMBER.items()}

# COMMAND ----------

# DBTITLE 1,Discover Bronze Tables for Sunpeak Inverters
# Get all bronze tables that match sunpeak_inverter_*_batch pattern
all_tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.bronze").collect()

# Inverter technical data tables only (no more power/energy tables)
inverter_bronze_tables = [
    row.tableName for row in all_tables
    if row.tableName.startswith('sunpeak_') and row.tableName.endswith('_batch')
]

if not inverter_bronze_tables:
    raise ValueError("No sunpeak_*_batch tables found in bronze layer")

print(f"Found {len(inverter_bronze_tables)} Sunpeak bronze tables")

# Extract serial_number from tables and map to site_id
import re
inverter_combinations = []
for table in inverter_bronze_tables:
    # Extract from sunpeak_{serial_number}_batch
    match = re.search(r'sunpeak_([a-z0-9_]+)_batch', table)
    if match:
        serial_number = match.group(1)
        # Query the bronze table to get the site_id (stored in id_site column)
        bronze_table = f"{CATALOG_NAME}.bronze.{table}"
        site_id_result = spark.sql(f"SELECT DISTINCT id_site FROM {bronze_table} LIMIT 1").collect()
        if site_id_result:
            site_id = site_id_result[0].id_site
            inverter_combinations.append({
                'site_id': site_id,
                'serial_number': serial_number
            })

print(f"\nInverter combinations: {len(inverter_combinations)}")
for combo in inverter_combinations:
    print(f"  - Site {combo['site_id']}, Serial {combo['serial_number']}")

# COMMAND ----------

# DBTITLE 1,Create Silver Tables for Inverter Technical Data
for combo in inverter_combinations:
    site_id = combo['site_id']
    serial_number = combo['serial_number']
    silver_table = f"{CATALOG_NAME}.silver.sunpeak_{serial_number}_batch"

    # Check if table exists
    table_exists = spark.catalog.tableExists(silver_table)

    if not table_exists:
        print(f"\nCreating silver inverter table: {silver_table}")

        spark.sql(f"""
        CREATE TABLE {silver_table} (
            timestamp TIMESTAMP COMMENT 'Telemetry measurement time',
            id_site STRING COMMENT 'Sunpeak site ID',
            serial_number STRING COMMENT 'Inverter serial number',

            -- AC Power and Energy - Ontology: W, WH
            ac_power DECIMAL(15,2) COMMENT 'Total active power in Watts',
            ac_power_l1 DECIMAL(15,2) COMMENT 'L1 active power in Watts',
            ac_energy_production DECIMAL(20,2) COMMENT 'Total lifetime energy in Watt-hours',

            -- AC Voltage and Current - Ontology: PhVphA, AphA
            ac_voltage_l1 DECIMAL(10,2) COMMENT 'L1 AC output voltage in Volts',
            ac_current_l1 DECIMAL(10,2) COMMENT 'L1 AC output current in Amperes',

            -- Frequency - Ontology: Hz
            ac_frequency DECIMAL(10,2) COMMENT 'L1 AC frequency in Hertz',

            -- Power Factor - Ontology: PF
            ac_power_factor DECIMAL(10,3) COMMENT 'L1 power factor (cos phi, dimensionless)',

            -- Apparent and Reactive Power - Ontology: VA, VAr
            ac_apparent_power DECIMAL(15,2) COMMENT 'L1 apparent power in VA',
            ac_reactive_power DECIMAL(15,2) COMMENT 'L1 reactive power in VAr',

            -- DC Voltage - Ontology: DCV
            dc_voltage DECIMAL(10,2) COMMENT 'DC voltage in Volts',

            -- Temperature - Ontology: TmpOt
            temperature_heat_sink DECIMAL(10,2) COMMENT 'Inverter temperature in Celsius',

            -- Operating State - Ontology: St
            operating_state STRING COMMENT 'Inverter operating mode (e.g., MPPT, OFF, SLEEPING)',

            -- Metadata
            model STRING COMMENT 'Inverter manufacturer (Sunpeak)',
            model_series STRING COMMENT 'Inverter model series (Single Phase)',
            model_sku STRING COMMENT 'Inverter model SKU from metadata',
            sk_inverter STRING COMMENT 'Device surrogate key - references d_inverters.sk_inverter'
        )
        CLUSTER BY AUTO
        COMMENT 'Curated inverter telemetry data for Sunpeak site_id {site_id}, serial_number {serial_number}, with Haystack ontology alignment.'
        """)

        print(f"Created table: {silver_table}")
    else:
        print(f"\nTable already exists: {silver_table}")

# COMMAND ----------

# DBTITLE 1,Process Inverter Technical Data
for combo in inverter_combinations:
    site_id = combo['site_id']
    serial_number = combo['serial_number']

    print(f"\n{'='*60}")
    print(f"Processing Inverter Data - Site: {site_id}, Serial: {serial_number}")
    print(f"{'='*60}")

    sanitized_sn = serial_number.replace('-', '_').lower()
    bronze_table = f"{CATALOG_NAME}.bronze.sunpeak_{sanitized_sn}_batch"
    silver_table = f"{CATALOG_NAME}.silver.sunpeak_{serial_number}_batch"
    inverters_dim_table = f"{CATALOG_NAME}.gold.d_inverters"

    # Get max timestamp from existing silver table
    max_ts_result = spark.sql(f"SELECT MAX(timestamp) as max_ts FROM {silver_table}").collect()[0]
    max_ts = max_ts_result.max_ts if max_ts_result.max_ts else None

    if max_ts:
        print(f"Latest timestamp in silver: {max_ts}")
        where_clause = f"WHERE b.timestamp > CAST('{max_ts}' AS TIMESTAMP)"
    else:
        print("No existing data - performing initial load")
        where_clause = "WHERE b.timestamp IS NOT NULL"

    # Build query with metadata join
    # Transform bronze fields to Haystack-aligned silver layer
    query = f"""
        SELECT
            b.timestamp,
            b.id_site AS id_site,
            b.serial_number AS serial_number,

            -- AC Power and Energy
            CAST(b.total_active_power AS DECIMAL(15,2)) AS ac_power,
            CAST(b.l1_active_power AS DECIMAL(15,2)) AS ac_power_l1,
            CAST(b.total_energy AS DECIMAL(20,2)) AS ac_energy_production,

            -- AC Voltage and Current
            CAST(b.l1_ac_voltage AS DECIMAL(10,2)) AS ac_voltage_l1,
            CAST(b.l1_ac_current AS DECIMAL(10,2)) AS ac_current_l1,

            -- Frequency
            CAST(b.l1_ac_frequency AS DECIMAL(10,2)) AS ac_frequency,

            -- Power Factor
            CAST(b.l1_cos_phi AS DECIMAL(10,3)) AS ac_power_factor,

            -- Apparent and Reactive Power
            CAST(b.l1_apparent_power AS DECIMAL(15,2)) AS ac_apparent_power,
            CAST(b.l1_reactive_power AS DECIMAL(15,2)) AS ac_reactive_power,

            -- DC Voltage
            CAST(b.dc_voltage AS DECIMAL(10,2)) AS dc_voltage,

            -- Temperature
            CAST(b.temperature AS DECIMAL(10,2)) AS temperature_heat_sink,

            -- Operating State
            b.inverter_mode AS operating_state,

            -- Metadata from d_inverters dimension table (dim now snake_case; silver still camelCase here, will migrate in Phase 2)
            d.brand AS model,
            d.model_series AS model_series,
            d.model_sku AS model_sku,
            d.sk_inverter AS sk_inverter
        FROM {bronze_table} b
        LEFT JOIN {inverters_dim_table} d
            ON d.id_site = b.id_site
            AND d.serial_number = b.serial_number
            AND d.connector = 'sunpeak_api'
        {where_clause}
    """

    # Execute query
    new_data_df = spark.sql(query)
    new_rows = new_data_df.count()

    if new_rows > 0:
        # Append new data
        new_data_df.write.mode("append").saveAsTable(silver_table)
        print(f"✓ Appended {new_rows:,} new rows to {silver_table}")
    else:
        print(f"✓ No new data to append for site {site_id}, serial {serial_number}")

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "="*60)
print("SILVER LAYER PROCESSING COMPLETE")
print("="*60)

print("\nInverter Technical Data Tables:")
for combo in inverter_combinations:
    site_id = combo['site_id']
    serial_number = combo['serial_number']
    silver_table = f"{CATALOG_NAME}.silver.sunpeak_{serial_number}_batch"
    row_count = spark.table(silver_table).count()
    print(f"  Site {site_id} Serial {serial_number}: {row_count:,} total rows")

print(f"\nNote:")
print(f"  - Power and energy metrics are derived from inverter technical data")
print(f"  - ac_power provides instantaneous power values")
print(f"  - Interval energy is calculated in the gold layer (dbt) using LAG window functions")
print(f"  - This approach supports per-inverter metrics and multiple inverters per site")
