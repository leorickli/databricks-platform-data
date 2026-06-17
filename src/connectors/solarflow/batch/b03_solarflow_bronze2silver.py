# Databricks notebook source
# DBTITLE 1,Imports and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# DBTITLE 1,Discover Bronze Tables for Solarflow Devices
# Get all bronze tables that match solarflow_{device_sn}_batch pattern
all_tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.bronze").collect()

# Pattern: solarflow_{device_sn}_batch (where device_sn is alphanumeric with underscores)
# This excludes old patterns like: solarflow_energy_plant_* or solarflow_inverter_plant_*
import re
solarflow_bronze_tables = []
for row in all_tables:
    table_name = row.tableName
    # Match exactly: solarflow_{device_sn}_batch
    # device_sn should be alphanumeric/underscores, not starting with 'energy' or 'inverter' or 'plant'
    if re.match(r'^solarflow_[a-z0-9_]+_batch$', table_name):
        # Exclude old naming patterns
        if not any(word in table_name for word in ['energy_plant', 'inverter_plant']):
            solarflow_bronze_tables.append(table_name)

if not solarflow_bronze_tables:
    raise ValueError("No solarflow_*_batch tables found in bronze layer")

print(f"Found {len(solarflow_bronze_tables)} Solarflow bronze tables:")
for table in solarflow_bronze_tables:
    print(f"  - {table}")

# Extract device_sn from table names and get plant_id from bronze data
import re
plant_device_combinations = []
for table in solarflow_bronze_tables:
    # Extract from solarflow_{device_sn}_batch
    match = re.search(r'solarflow_([a-z0-9_]+)_batch', table)
    if match:
        device_sn = match.group(1)
        # Query the bronze table to get plant_id
        plant_id_result = spark.sql(f"""
            SELECT DISTINCT id_plant
            FROM {CATALOG_NAME}.bronze.{table}
            LIMIT 1
        """).collect()

        if plant_id_result and plant_id_result[0]['id_plant']:
            plant_id = plant_id_result[0]['id_plant']
            plant_device_combinations.append({
                "plant_id": plant_id,
                "device_sn": device_sn,
                "table_name": table
            })
        else:
            print(f"Warning: Could not determine plant_id for {table}")

print(f"\nFound {len(plant_device_combinations)} plant/device combination(s)")
for combo in plant_device_combinations:
    print(f"  - Plant {combo['plant_id']}, Device {combo['device_sn']}")

# COMMAND ----------

# DBTITLE 1,Create Silver Inverter Tables Per Plant/Device
for combo in plant_device_combinations:
    plant_id = combo['plant_id']
    device_sn = combo['device_sn']
    silver_table = f"{CATALOG_NAME}.silver.solarflow_{device_sn}_batch"

    # Check if table exists
    table_exists = spark.catalog.tableExists(silver_table)

    if not table_exists:
        print(f"\nCreating silver inverter table: {silver_table}")

        spark.sql(f"""
        CREATE TABLE {silver_table} (
            timestamp TIMESTAMP COMMENT 'Measurement timestamp from the inverter',
            id_site STRING COMMENT 'Solarflow plant/site ID',

            -- AC Power Output (W) - Ontology: MicroInverter.ratedPower
            ac_power DECIMAL(10,2) COMMENT 'Total AC output power (W)',
            ac_power_l1 DECIMAL(10,2) COMMENT 'AC output power phase 1 (W)',
            ac_power_l2 DECIMAL(10,2) COMMENT 'AC output power phase 2 (W)',
            ac_power_l3 DECIMAL(10,2) COMMENT 'AC output power phase 3 (W)',

            -- PV Input Power (W) - Ontology: MicroInverter.panelAssociation (per string MPPT)
            dc_power DECIMAL(10,2) COMMENT 'Total PV input power (W)',
            dc_power_string_1 DECIMAL(10,2) COMMENT 'PV string 1 input power (W)',
            dc_power_string_2 DECIMAL(10,2) COMMENT 'PV string 2 input power (W)',
            dc_power_string_3 DECIMAL(10,2) COMMENT 'PV string 3 input power (W)',
            dc_power_string_4 DECIMAL(10,2) COMMENT 'PV string 4 input power (W)',

            -- Energy Generation (kWh) - Ontology: Energy Measurement
            energy_production_daily DECIMAL(10,3) COMMENT 'AC energy generated today (kWh)',

            -- AC Voltage Output (V) - Ontology: MicroInverter.outputVoltage
            ac_voltage_l1 DECIMAL(10,2) COMMENT 'AC voltage phase 1 (V)',
            ac_voltage_l2 DECIMAL(10,2) COMMENT 'AC voltage phase 2 (V)',
            ac_voltage_l3 DECIMAL(10,2) COMMENT 'AC voltage phase 3 (V)',

            -- PV Input Voltage (V) - Ontology: MicroInverter.inputVoltageRange
            dc_voltage_string_1 DECIMAL(10,2) COMMENT 'PV string 1 voltage (V)',
            dc_voltage_string_2 DECIMAL(10,2) COMMENT 'PV string 2 voltage (V)',
            dc_voltage_string_3 DECIMAL(10,2) COMMENT 'PV string 3 voltage (V)',
            dc_voltage_string_4 DECIMAL(10,2) COMMENT 'PV string 4 voltage (V)',

            -- AC Current Output (A)
            ac_current_l1 DECIMAL(10,2) COMMENT 'AC current phase 1 (A)',
            ac_current_l2 DECIMAL(10,2) COMMENT 'AC current phase 2 (A)',
            ac_current_l3 DECIMAL(10,2) COMMENT 'AC current phase 3 (A)',

            -- PV Input Current (A)
            dc_current_string_1 DECIMAL(10,2) COMMENT 'PV string 1 current (A)',
            dc_current_string_2 DECIMAL(10,2) COMMENT 'PV string 2 current (A)',
            dc_current_string_3 DECIMAL(10,2) COMMENT 'PV string 3 current (A)',
            dc_current_string_4 DECIMAL(10,2) COMMENT 'PV string 4 current (A)',

            -- Frequency (Hz) - Ontology: Hz
            ac_frequency DECIMAL(10,2) COMMENT 'AC frequency (Hz)',

            -- Power Factor - Ontology: PF
            ac_power_factor DECIMAL(10,3) COMMENT 'Power factor (dimensionless)',

            -- Phase-to-Phase Voltage (V) - Ontology: PPVphAB, PPVphBC, PPVphCA
            ac_voltage_l1_l2 DECIMAL(10,2) COMMENT 'AC voltage R-S / Phase AB (V)',
            ac_voltage_l2_l3 DECIMAL(10,2) COMMENT 'AC voltage S-T / Phase BC (V)',
            ac_voltage_l3_l1 DECIMAL(10,2) COMMENT 'AC voltage T-R / Phase CA (V)',

            -- Operating Status - Ontology: St
            operating_state STRING COMMENT 'Inverter operating_state code (0=Waiting, 1=Normal, 3=Fault)',

            -- Temperature (°C)
            temp1 DECIMAL(10,2) COMMENT 'Temperature sensor 1 (°C)',
            temp2 DECIMAL(10,2) COMMENT 'Temperature sensor 2 (°C)',
            temp5 DECIMAL(10,2) COMMENT 'Temperature sensor 5 (°C)',

            -- Grid and Load Energy Flow (kWh) - Daily metrics only
            energy_export_daily DECIMAL(10,3) COMMENT 'Energy exported to grid today (kWh)',
            energy_consumption_daily DECIMAL(10,3) COMMENT 'Local building consumption today (kWh)',
            energy_self_consumption_daily DECIMAL(10,3) COMMENT 'Solar self-consumption today (kWh)',

            -- Inverter metadata (hardcoded) - Ontology: MicroInverter attributes
            model STRING COMMENT 'Inverter brand/manufacturer - Ontology: MicroInverter.manufacturer',
            model_series STRING COMMENT 'Inverter model series - Ontology: MicroInverter.model',
            model_sku STRING COMMENT 'Inverter model SKU',

            -- Surrogate key for joins with dimension tables
            sk_inverter STRING COMMENT 'Device surrogate key - references d_inverters.sk_inverter'
        )
        CLUSTER BY AUTO
        COMMENT 'MicroInverter telemetry from Solarflow device (Plant {plant_id}, SN {device_sn}) following energy ontology MicroInverter class.'
        """)

        print(f"✓ Created inverter table: {silver_table}")
    else:
        print(f"Inverter table already exists: {silver_table}")

# COMMAND ----------

# DBTITLE 1,Process Inverter Data Per Plant/Device
for combo in plant_device_combinations:
    plant_id = combo['plant_id']
    device_sn = combo['device_sn']
    bronze_table = f"{CATALOG_NAME}.bronze.{combo['table_name']}"
    silver_table = f"{CATALOG_NAME}.silver.solarflow_{device_sn}_batch"
    inverters_dim_table = f"{CATALOG_NAME}.gold.d_inverters"

    print(f"\n{'='*60}")
    print(f"Processing Inverter - Plant {plant_id}, Device {device_sn}")
    print(f"{'='*60}")

    # Get max timestamp from existing silver table
    max_ts_result = spark.sql(f"SELECT MAX(timestamp) as max_ts FROM {silver_table}").collect()[0]
    max_ts = max_ts_result.max_ts if max_ts_result.max_ts else None

    if max_ts:
        print(f"Latest timestamp in silver: {max_ts}")
        where_clause = f"WHERE b.timestamp > CAST('{max_ts}' AS TIMESTAMP)"
    else:
        print("No existing data - performing initial load")
        where_clause = "WHERE b.timestamp IS NOT NULL"

    # Build transformation query for inverter
    # Use Haystack-compliant id_site format to match dimension tables
    transformation_query = f"""
    SELECT
        b.timestamp,
        CONCAT('solarflow-', CAST(b.id_plant AS STRING)) AS id_site,

        -- AC Power Output (W)
        CAST(b.pac AS DECIMAL(10,2)) AS ac_power,
        CAST(b.pac1 AS DECIMAL(10,2)) AS ac_power_l1,
        CAST(b.pac2 AS DECIMAL(10,2)) AS ac_power_l2,
        CAST(b.pac3 AS DECIMAL(10,2)) AS ac_power_l3,

        -- PV Input Power (W) - per string MPPT
        CAST(b.ppv AS DECIMAL(10,2)) AS dc_power,
        CAST(b.ppv1 AS DECIMAL(10,2)) AS dc_power_string_1,
        CAST(b.ppv2 AS DECIMAL(10,2)) AS dc_power_string_2,
        CAST(b.ppv3 AS DECIMAL(10,2)) AS dc_power_string_3,
        CAST(b.ppv4 AS DECIMAL(10,2)) AS dc_power_string_4,

        -- Energy Generation (kWh)
        CAST(b.eac_today AS DECIMAL(10,3)) AS energy_production_daily,

        -- AC Voltage Output (V)
        CAST(b.vac1 AS DECIMAL(10,2)) AS ac_voltage_l1,
        CAST(b.vac2 AS DECIMAL(10,2)) AS ac_voltage_l2,
        CAST(b.vac3 AS DECIMAL(10,2)) AS ac_voltage_l3,

        -- PV Input Voltage (V)
        CAST(b.vpv1 AS DECIMAL(10,2)) AS dc_voltage_string_1,
        CAST(b.vpv2 AS DECIMAL(10,2)) AS dc_voltage_string_2,
        CAST(b.vpv3 AS DECIMAL(10,2)) AS dc_voltage_string_3,
        CAST(b.vpv4 AS DECIMAL(10,2)) AS dc_voltage_string_4,

        -- AC Current Output (A)
        CAST(b.iac1 AS DECIMAL(10,2)) AS ac_current_l1,
        CAST(b.iac2 AS DECIMAL(10,2)) AS ac_current_l2,
        CAST(b.iac3 AS DECIMAL(10,2)) AS ac_current_l3,

        -- PV Input Current (A)
        CAST(b.ipv1 AS DECIMAL(10,2)) AS dc_current_string_1,
        CAST(b.ipv2 AS DECIMAL(10,2)) AS dc_current_string_2,
        CAST(b.ipv3 AS DECIMAL(10,2)) AS dc_current_string_3,
        CAST(b.ipv4 AS DECIMAL(10,2)) AS dc_current_string_4,

        -- Frequency (Hz)
        CAST(b.fac AS DECIMAL(10,2)) AS ac_frequency,

        -- Power Factor
        CAST(b.pf AS DECIMAL(10,3)) AS ac_power_factor,

        -- Phase-to-Phase Voltage (V)
        CAST(b.vac_rs AS DECIMAL(10,2)) AS ac_voltage_l1_l2,
        CAST(b.vac_st AS DECIMAL(10,2)) AS ac_voltage_l2_l3,
        CAST(b.vac_tr AS DECIMAL(10,2)) AS ac_voltage_l3_l1,

        -- Operating Status
        b.status AS operating_state,

        -- Temperature (°C)
        CAST(b.temp1 AS DECIMAL(10,2)) AS temp1,
        CAST(b.temp2 AS DECIMAL(10,2)) AS temp2,
        CAST(b.temp5 AS DECIMAL(10,2)) AS temp5,

        -- Grid and Load Energy Flow (kWh) - daily metrics only
        CAST(b.eto_grid_today AS DECIMAL(10,3)) AS energy_export_daily,
        CAST(b.elocal_load_today AS DECIMAL(10,3)) AS energy_consumption_daily,
        CAST(b.eself_today AS DECIMAL(10,3)) AS energy_self_consumption_daily,

        -- Metadata from d_inverters dimension table (dim now snake_case; silver still camelCase here, will migrate in Phase 3)
        d.brand AS model,
        d.model_series AS model_series,
        d.model_sku AS model_sku,
        d.sk_inverter AS sk_inverter

    FROM {bronze_table} b
    LEFT JOIN {inverters_dim_table} d
        ON d.id_site = CONCAT('solarflow-', CAST(b.id_plant AS STRING))
        AND UPPER(d.serial_number) = UPPER('{device_sn}')
        AND d.connector = 'solarflow_api'
    {where_clause}
    """

    # Execute transformation
    new_data_df = spark.sql(transformation_query)
    new_rows = new_data_df.count()

    if new_rows > 0:
        # Append new data
        new_data_df.write.mode("append").saveAsTable(silver_table)
        print(f"✓ Appended {new_rows:,} new rows to inverter table")
    else:
        print(f"✓ No new inverter data to append")

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "="*60)
print("SILVER LAYER PROCESSING COMPLETE")
print("="*60)

for combo in plant_device_combinations:
    plant_id = combo['plant_id']
    device_sn = combo['device_sn']

    silver_table = f"{CATALOG_NAME}.silver.solarflow_{device_sn}_batch"
    row_count = spark.table(silver_table).count()

    print(f"Plant {plant_id}, Device {device_sn}: {row_count:,} rows")
