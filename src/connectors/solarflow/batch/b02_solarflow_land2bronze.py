# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "solarflow_inverters_batch", "Volume Name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}/"

CHECKPOINT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/"

# COMMAND ----------

# DBTITLE 1,Create Bronze Table Function
def create_bronze_table(catalog_name, plant_id, device_sn):
    """Create bronze table for a specific plant_id and device serial number if it doesn't exist."""
    # Sanitize serial number for table name
    sanitized_sn = device_sn.replace('-', '_').replace(' ', '_').lower()
    table_name = f"solarflow_{sanitized_sn}_batch"
    table_path = f"{catalog_name}.bronze.{table_name}"

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
      timestamp TIMESTAMP COMMENT 'Energy measurement time from the time field',
      id_plant STRING COMMENT 'Solarflow plant/site ID',
      serial_num STRING COMMENT 'Inverter serial number',
      data_log_sn STRING COMMENT 'Data logger serial number',
      status STRING COMMENT 'Device status code',
      status_text STRING COMMENT 'Device status description',

      -- Calendar struct for timezone information
      calendar STRUCT<
        calendar_type: STRING,
        first_day_of_week: STRING,
        minimal_days_in_first_week: STRING,
        gregorian_change: STRUCT<
          date: STRING,
          hours: STRING,
          seconds: STRING,
          month: STRING,
          timezone_offset: STRING,
          year: STRING,
          minutes: STRING,
          time: STRING,
          day: STRING
        >,
        time_zone: STRUCT<
          dirty: STRING,
          last_rule_instance: STRING,
          dst_savings: STRING,
          display_name: STRING,
          raw_offset: STRING,
          id: STRING
        >,
        week_year: STRING,
        time: STRUCT<
          date: STRING,
          hours: STRING,
          seconds: STRING,
          month: STRING,
          timezone_offset: STRING,
          year: STRING,
          minutes: STRING,
          time: STRING,
          day: STRING
        >,
        time_in_millis: STRING,
        weeks_in_week_year: STRING,
        lenient: STRING,
        week_date_supported: STRING
      > COMMENT 'Calendar object with timezone and time information',

      -- Power and energy measurements (STRING type for bronze)
      pac STRING COMMENT 'AC output power (W)',
      pac1 STRING COMMENT 'AC output power phase 1 (W)',
      pac2 STRING COMMENT 'AC output power phase 2 (W)',
      pac3 STRING COMMENT 'AC output power phase 3 (W)',
      ppv STRING COMMENT 'PV input power (W)',
      ppv1 STRING COMMENT 'PV input power string 1 (W)',
      ppv2 STRING COMMENT 'PV input power string 2 (W)',
      ppv3 STRING COMMENT 'PV input power string 3 (W)',
      ppv4 STRING COMMENT 'PV input power string 4 (W)',

      -- Energy totals
      eac_today STRING COMMENT 'AC energy generated today (kWh)',
      eac_total STRING COMMENT 'Total AC energy generated (kWh)',
      epv1_today STRING COMMENT 'PV1 energy today (kWh)',
      epv1_total STRING COMMENT 'PV1 total energy (kWh)',
      epv2_today STRING COMMENT 'PV2 energy today (kWh)',
      epv2_total STRING COMMENT 'PV2 total energy (kWh)',
      epv3_today STRING COMMENT 'PV3 energy today (kWh)',
      epv3_total STRING COMMENT 'PV3 total energy (kWh)',
      epv4_today STRING COMMENT 'PV4 energy today (kWh)',
      epv4_total STRING COMMENT 'PV4 total energy (kWh)',
      epv_total STRING COMMENT 'Total PV energy (kWh)',

      -- Grid and load
      eac_charge_today STRING COMMENT 'AC charge energy today (kWh)',
      eac_charge_total STRING COMMENT 'Total AC charge energy (kWh)',
      echarge_today STRING COMMENT 'Charge energy today (kWh)',
      echarge_total STRING COMMENT 'Total charge energy (kWh)',
      edischarge_today STRING COMMENT 'Discharge energy today (kWh)',
      edischarge_total STRING COMMENT 'Total discharge energy (kWh)',
      eto_grid_today STRING COMMENT 'Energy to grid today (kWh)',
      eto_grid_total STRING COMMENT 'Total energy to grid (kWh)',
      eto_user_today STRING COMMENT 'Energy to user today (kWh)',
      eto_user_total STRING COMMENT 'Total energy to user (kWh)',
      elocal_load_today STRING COMMENT 'Local load energy today (kWh)',
      elocal_load_total STRING COMMENT 'Total local load energy (kWh)',
      eself_today STRING COMMENT 'Self consumption today (kWh)',
      eself_total STRING COMMENT 'Total self consumption (kWh)',
      esystem_today STRING COMMENT 'System energy today (kWh)',
      esystem_total STRING COMMENT 'Total system energy (kWh)',
      eex1_today STRING COMMENT 'Export 1 energy today (kWh)',
      eex1_total STRING COMMENT 'Total export 1 energy (kWh)',
      eex2_today STRING COMMENT 'Export 2 energy today (kWh)',
      eex2_total STRING COMMENT 'Total export 2 energy (kWh)',

      -- Power distribution
      pac_to_grid_total STRING COMMENT 'Total AC power to grid (W)',
      pac_to_local_load STRING COMMENT 'AC power to local load (W)',
      pac_to_user_total STRING COMMENT 'Total AC power to user (W)',
      pself STRING COMMENT 'Self consumption power (W)',
      psystem STRING COMMENT 'System power (W)',
      pex1 STRING COMMENT 'Export 1 power (W)',
      pex2 STRING COMMENT 'Export 2 power (W)',
      pacr STRING COMMENT 'AC reactive power (W)',

      -- Voltage measurements
      vac1 STRING COMMENT 'AC voltage phase 1 (V)',
      vac2 STRING COMMENT 'AC voltage phase 2 (V)',
      vac3 STRING COMMENT 'AC voltage phase 3 (V)',
      vac_rs STRING COMMENT 'AC voltage R-S (V)',
      vac_st STRING COMMENT 'AC voltage S-T (V)',
      vac_tr STRING COMMENT 'AC voltage T-R (V)',
      vacr STRING COMMENT 'AC voltage R (V)',
      vacrs STRING COMMENT 'AC voltage RS (V)',
      vpv1 STRING COMMENT 'PV voltage string 1 (V)',
      vpv2 STRING COMMENT 'PV voltage string 2 (V)',
      vpv3 STRING COMMENT 'PV voltage string 3 (V)',
      vpv4 STRING COMMENT 'PV voltage string 4 (V)',
      dc_voltage STRING COMMENT 'DC voltage (V)',
      p_bus_voltage STRING COMMENT 'Positive bus voltage (V)',
      n_bus_voltage STRING COMMENT 'Negative bus voltage (V)',

      -- Current measurements
      iac1 STRING COMMENT 'AC current phase 1 (A)',
      iac2 STRING COMMENT 'AC current phase 2 (A)',
      iac3 STRING COMMENT 'AC current phase 3 (A)',
      iacr STRING COMMENT 'AC current R (A)',
      ipv1 STRING COMMENT 'PV current string 1 (A)',
      ipv2 STRING COMMENT 'PV current string 2 (A)',
      ipv3 STRING COMMENT 'PV current string 3 (A)',
      ipv4 STRING COMMENT 'PV current string 4 (A)',
      dci_r STRING COMMENT 'DC current R (A)',
      dci_s STRING COMMENT 'DC current S (A)',
      dci_t STRING COMMENT 'DC current T (A)',

      -- Frequency and power factor
      fac STRING COMMENT 'AC frequency (Hz)',
      pf STRING COMMENT 'Power factor',

      -- Temperature
      temp1 STRING COMMENT 'Temperature sensor 1 (°C)',
      temp2 STRING COMMENT 'Temperature sensor 2 (°C)',
      temp3 STRING COMMENT 'Temperature sensor 3 (°C)',
      temp4 STRING COMMENT 'Temperature sensor 4 (°C)',
      temp5 STRING COMMENT 'Temperature sensor 5 (°C)',

      -- Battery DC (BDC) system - BDC1
      bdc1_charge_power STRING COMMENT 'BDC1 charge power (W)',
      bdc1_charge_total STRING COMMENT 'BDC1 total charge energy (kWh)',
      bdc1_discharge_power STRING COMMENT 'BDC1 discharge power (W)',
      bdc1_discharge_total STRING COMMENT 'BDC1 total discharge energy (kWh)',
      bdc1_fault_type STRING COMMENT 'BDC1 fault type code',
      bdc1_ibat STRING COMMENT 'BDC1 battery current (A)',
      bdc1_ibb STRING COMMENT 'BDC1 IBB current (A)',
      bdc1_illc STRING COMMENT 'BDC1 LLC current (A)',
      bdc1_mode STRING COMMENT 'BDC1 operating mode',
      bdc1_soc STRING COMMENT 'BDC1 state of charge (%)',
      bdc1_status STRING COMMENT 'BDC1 status code',
      bdc1_temp1 STRING COMMENT 'BDC1 temperature 1 (°C)',
      bdc1_temp2 STRING COMMENT 'BDC1 temperature 2 (°C)',
      bdc1_vbat STRING COMMENT 'BDC1 battery voltage (V)',
      bdc1_vbus1 STRING COMMENT 'BDC1 bus voltage 1 (V)',
      bdc1_vbus2 STRING COMMENT 'BDC1 bus voltage 2 (V)',
      bdc1_warn_code STRING COMMENT 'BDC1 warning code',

      -- Battery DC (BDC) system - BDC2
      bdc2_charge_power STRING COMMENT 'BDC2 charge power (W)',
      bdc2_charge_total STRING COMMENT 'BDC2 total charge energy (kWh)',
      bdc2_discharge_power STRING COMMENT 'BDC2 discharge power (W)',
      bdc2_discharge_total STRING COMMENT 'BDC2 total discharge energy (kWh)',
      bdc2_fault_type STRING COMMENT 'BDC2 fault type code',
      bdc2_ibat STRING COMMENT 'BDC2 battery current (A)',
      bdc2_ibb STRING COMMENT 'BDC2 IBB current (A)',
      bdc2_illc STRING COMMENT 'BDC2 LLC current (A)',
      bdc2_mode STRING COMMENT 'BDC2 operating mode',
      bdc2_soc STRING COMMENT 'BDC2 state of charge (%)',
      bdc2_status STRING COMMENT 'BDC2 status code',
      bdc2_temp1 STRING COMMENT 'BDC2 temperature 1 (°C)',
      bdc2_temp2 STRING COMMENT 'BDC2 temperature 2 (°C)',
      bdc2_vbat STRING COMMENT 'BDC2 battery voltage (V)',
      bdc2_vbus1 STRING COMMENT 'BDC2 bus voltage 1 (V)',
      bdc2_vbus2 STRING COMMENT 'BDC2 bus voltage 2 (V)',
      bdc2_warn_code STRING COMMENT 'BDC2 warning code',

      -- BDC common
      bdc_bus_ref STRING COMMENT 'BDC bus reference voltage (V)',
      bdc_derate_reason STRING COMMENT 'BDC derate reason code',
      bdc_fault_sub_code STRING COMMENT 'BDC fault sub code',
      bdc_status STRING COMMENT 'BDC status code',
      bdc_vbus2_neg STRING COMMENT 'BDC bus 2 negative voltage (V)',
      bdc_warn_sub_code STRING COMMENT 'BDC warning sub code',

      -- Battery Management System (BMS)
      bms_communication_type STRING COMMENT 'BMS communication type',
      bms_cv_volt STRING COMMENT 'BMS constant voltage (V)',
      bms_error2 STRING COMMENT 'BMS error code 2',
      bms_error3 STRING COMMENT 'BMS error code 3',
      bms_error4 STRING COMMENT 'BMS error code 4',
      bms_fault_type STRING COMMENT 'BMS fault type code',
      bms_fw_version STRING COMMENT 'BMS firmware version',
      bms_ibat STRING COMMENT 'BMS battery current (A)',
      bms_icycle STRING COMMENT 'BMS current cycle',
      bms_info STRING COMMENT 'BMS info code',
      bms_ios_status STRING COMMENT 'BMS IOS status',
      bms_max_curr STRING COMMENT 'BMS maximum current (A)',
      bms_mcu_version STRING COMMENT 'BMS MCU version',
      bms_pack_info STRING COMMENT 'BMS pack information',
      bms_soc STRING COMMENT 'BMS state of charge (%)',
      bms_soh STRING COMMENT 'BMS state of health (%)',
      bms_status STRING COMMENT 'BMS status code',
      bms_temp1_bat STRING COMMENT 'BMS battery temperature (°C)',
      bms_using_cap STRING COMMENT 'BMS using capacity (Ah)',
      bms_vbat STRING COMMENT 'BMS battery voltage (V)',
      bms_vdelta STRING COMMENT 'BMS voltage delta (V)',
      bms_warn2 STRING COMMENT 'BMS warning 2',
      bms_warn_code STRING COMMENT 'BMS warning code',

      -- Battery info
      bat_sn STRING COMMENT 'Battery serial number',
      battery_no STRING COMMENT 'Battery number',
      battery_sn STRING COMMENT 'Battery SN',
      soc1 STRING COMMENT 'State of charge 1 (%)',
      soc2 STRING COMMENT 'State of charge 2 (%)',

      -- EPS (Emergency Power Supply)
      eps_fac STRING COMMENT 'EPS AC frequency (Hz)',
      eps_iac1 STRING COMMENT 'EPS AC current phase 1 (A)',
      eps_iac2 STRING COMMENT 'EPS AC current phase 2 (A)',
      eps_iac3 STRING COMMENT 'EPS AC current phase 3 (A)',
      eps_pac STRING COMMENT 'EPS AC power (W)',
      eps_pac1 STRING COMMENT 'EPS AC power phase 1 (W)',
      eps_pac2 STRING COMMENT 'EPS AC power phase 2 (W)',
      eps_pac3 STRING COMMENT 'EPS AC power phase 3 (W)',
      eps_pf STRING COMMENT 'EPS power factor',
      eps_vac1 STRING COMMENT 'EPS AC voltage phase 1 (V)',
      eps_vac2 STRING COMMENT 'EPS AC voltage phase 2 (V)',
      eps_vac3 STRING COMMENT 'EPS AC voltage phase 3 (V)',

      -- Fault and warning codes
      fault_type STRING COMMENT 'Fault type code',
      fault_type1 STRING COMMENT 'Fault type 1 code',
      warn_code STRING COMMENT 'Warning code',
      warn_code1 STRING COMMENT 'Warning code 1',
      warn_text STRING COMMENT 'Warning text description',
      error_text STRING COMMENT 'Error text description',
      new_warn_code STRING COMMENT 'New warning code',
      new_warn_sub_code STRING COMMENT 'New warning sub code',
      sys_fault_word STRING COMMENT 'System fault word',
      sys_fault_word1 STRING COMMENT 'System fault word 1',
      sys_fault_word2 STRING COMMENT 'System fault word 2',
      sys_fault_word3 STRING COMMENT 'System fault word 3',
      sys_fault_word4 STRING COMMENT 'System fault word 4',
      sys_fault_word5 STRING COMMENT 'System fault word 5',
      sys_fault_word6 STRING COMMENT 'System fault word 6',
      sys_fault_word7 STRING COMMENT 'System fault word 7',

      -- Operating modes and status
      operating_mode STRING COMMENT 'Operating mode code',
      derating_mode STRING COMMENT 'Derating mode code',
      bsystem_work_mode STRING COMMENT 'B system work mode',
      uw_sys_work_mode STRING COMMENT 'UW system work mode',
      bgrid_type STRING COMMENT 'B grid type',

      -- Other operational parameters
      address STRING COMMENT 'Device address',
      alias STRING COMMENT 'Device alias',
      again STRING COMMENT 'Again flag',
      is_again STRING COMMENT 'Is again flag',
      b_merter_connect_flag STRING COMMENT 'B meter connect flag',
      day STRING COMMENT 'Day field',
      dry_contact_status STRING COMMENT 'Dry contact status',
      gfci STRING COMMENT 'Ground fault circuit interrupter',
      inv_delay_time STRING COMMENT 'Inverter delay time (s)',
      iso STRING COMMENT 'Isolation resistance (Ω)',
      load_percent STRING COMMENT 'Load percentage (%)',
      lost STRING COMMENT 'Lost connection flag',
      mtnc_mode STRING COMMENT 'Maintenance mode',
      mtnc_rqst STRING COMMENT 'Maintenance request',
      op_fullwatt STRING COMMENT 'Operation full watt',
      real_op_percent STRING COMMENT 'Real operation percentage (%)',
      t_mtnc_strt STRING COMMENT 'Maintenance start time',
      t_win_end STRING COMMENT 'Window end time',
      t_win_start STRING COMMENT 'Window start time',
      time_total STRING COMMENT 'Total operating time (s)',
      total_working_time STRING COMMENT 'Total working time (s)',
      tlx_bean STRING COMMENT 'TLX bean data',
      win_mode STRING COMMENT 'Window mode',
      win_off_grid_soc STRING COMMENT 'Window off-grid SOC (%)',
      win_on_grid_soc STRING COMMENT 'Window on-grid SOC (%)',
      win_request STRING COMMENT 'Window request',
      with_time STRING COMMENT 'With time flag',

      -- Processing metadata
      source_file STRING COMMENT 'Source JSON file in the landing volume for lineage',
      bronze_processing_timestamp TIMESTAMP COMMENT 'When the record was processed in the bronze layer'
    )
    CLUSTER BY AUTO
    COMMENT 'Stores Solarflow energy measurements for device (Plant {plant_id}, SN {device_sn}).'
    """)

    return table_path

# COMMAND ----------

# DBTITLE 1,Processing Function for Solarflow Energy Data
def process_solarflow_batch(batch_df, batch_id, plant_id, device_sn):
    """
    Process batch of Solarflow energy JSON files for a specific plant_id and device serial number.
    Extracts all fields and stores them as strings (except timestamps).
    """
    # Filter files for this 
    site_files = batch_df.filter(
        F.col("device_sn") == device_sn
    ).select("_metadata.file_path").distinct().collect()

    if not site_files:
        return

    print(f"  Plant {plant_id} Device {device_sn}: Processing {len(site_files)} file(s)")

    all_rows = []

    for file_row in site_files:
        file_path = file_row['file_path']
        filename = os.path.basename(file_path)

        print(f"    Processing {filename}")

        try:
            # Read the JSON file content
            with open(file_path.replace('dbfs:', '/dbfs'), 'r') as f:
                data = json.load(f)

            # Extract main timestamp
            timestamp = data.get('time')

            # Extract calendar struct (keep nested structure)
            calendar_data = data.get('calendar', {})

            # Helper function to safely convert to string
            def to_str(value):
                if value is None or value == "":
                    return None
                return str(value)

            # Build the row with all fields as strings
            row = {
                "timestamp": timestamp,
                "id_plant": plant_id,
                "serial_num": to_str(data.get('serialNum')),
                "data_log_sn": to_str(data.get('dataLogSn')),
                "status": to_str(data.get('status')),
                "status_text": to_str(data.get('statusText')),

                # Calendar struct - extract nested structure
                "calendar": calendar_data if calendar_data else None,

                # Power measurements
                "pac": to_str(data.get('pac')),
                "pac1": to_str(data.get('pac1')),
                "pac2": to_str(data.get('pac2')),
                "pac3": to_str(data.get('pac3')),
                "ppv": to_str(data.get('ppv')),
                "ppv1": to_str(data.get('ppv1')),
                "ppv2": to_str(data.get('ppv2')),
                "ppv3": to_str(data.get('ppv3')),
                "ppv4": to_str(data.get('ppv4')),

                # Energy totals
                "eac_today": to_str(data.get('eacToday')),
                "eac_total": to_str(data.get('eacTotal')),
                "epv1_today": to_str(data.get('epv1Today')),
                "epv1_total": to_str(data.get('epv1Total')),
                "epv2_today": to_str(data.get('epv2Today')),
                "epv2_total": to_str(data.get('epv2Total')),
                "epv3_today": to_str(data.get('epv3Today')),
                "epv3_total": to_str(data.get('epv3Total')),
                "epv4_today": to_str(data.get('epv4Today')),
                "epv4_total": to_str(data.get('epv4Total')),
                "epv_total": to_str(data.get('epvTotal')),

                # Grid and load
                "eac_charge_today": to_str(data.get('eacChargeToday')),
                "eac_charge_total": to_str(data.get('eacChargeTotal')),
                "echarge_today": to_str(data.get('echargeToday')),
                "echarge_total": to_str(data.get('echargeTotal')),
                "edischarge_today": to_str(data.get('edischargeToday')),
                "edischarge_total": to_str(data.get('edischargeTotal')),
                "eto_grid_today": to_str(data.get('etoGridToday')),
                "eto_grid_total": to_str(data.get('etoGridTotal')),
                "eto_user_today": to_str(data.get('etoUserToday')),
                "eto_user_total": to_str(data.get('etoUserTotal')),
                "elocal_load_today": to_str(data.get('elocalLoadToday')),
                "elocal_load_total": to_str(data.get('elocalLoadTotal')),
                "eself_today": to_str(data.get('eselfToday')),
                "eself_total": to_str(data.get('eselfTotal')),
                "esystem_today": to_str(data.get('esystemToday')),
                "esystem_total": to_str(data.get('esystemTotal')),
                "eex1_today": to_str(data.get('eex1Today')),
                "eex1_total": to_str(data.get('eex1Total')),
                "eex2_today": to_str(data.get('eex2Today')),
                "eex2_total": to_str(data.get('eex2Total')),

                # Power distribution
                "pac_to_grid_total": to_str(data.get('pacToGridTotal')),
                "pac_to_local_load": to_str(data.get('pacToLocalLoad')),
                "pac_to_user_total": to_str(data.get('pacToUserTotal')),
                "pself": to_str(data.get('pself')),
                "psystem": to_str(data.get('psystem')),
                "pex1": to_str(data.get('pex1')),
                "pex2": to_str(data.get('pex2')),
                "pacr": to_str(data.get('pacr')),

                # Voltage measurements
                "vac1": to_str(data.get('vac1')),
                "vac2": to_str(data.get('vac2')),
                "vac3": to_str(data.get('vac3')),
                "vac_rs": to_str(data.get('vacRs')),
                "vac_st": to_str(data.get('vacSt')),
                "vac_tr": to_str(data.get('vacTr')),
                "vacr": to_str(data.get('vacr')),
                "vacrs": to_str(data.get('vacrs')),
                "vpv1": to_str(data.get('vpv1')),
                "vpv2": to_str(data.get('vpv2')),
                "vpv3": to_str(data.get('vpv3')),
                "vpv4": to_str(data.get('vpv4')),
                "dc_voltage": to_str(data.get('dcVoltage')),
                "p_bus_voltage": to_str(data.get('pBusVoltage')),
                "n_bus_voltage": to_str(data.get('nBusVoltage')),

                # Current measurements
                "iac1": to_str(data.get('iac1')),
                "iac2": to_str(data.get('iac2')),
                "iac3": to_str(data.get('iac3')),
                "iacr": to_str(data.get('iacr')),
                "ipv1": to_str(data.get('ipv1')),
                "ipv2": to_str(data.get('ipv2')),
                "ipv3": to_str(data.get('ipv3')),
                "ipv4": to_str(data.get('ipv4')),
                "dci_r": to_str(data.get('dciR')),
                "dci_s": to_str(data.get('dciS')),
                "dci_t": to_str(data.get('dciT')),

                # Frequency and power factor
                "fac": to_str(data.get('fac')),
                "pf": to_str(data.get('pf')),

                # Temperature
                "temp1": to_str(data.get('temp1')),
                "temp2": to_str(data.get('temp2')),
                "temp3": to_str(data.get('temp3')),
                "temp4": to_str(data.get('temp4')),
                "temp5": to_str(data.get('temp5')),

                # BDC1 fields
                "bdc1_charge_power": to_str(data.get('bdc1ChargePower')),
                "bdc1_charge_total": to_str(data.get('bdc1ChargeTotal')),
                "bdc1_discharge_power": to_str(data.get('bdc1DischargePower')),
                "bdc1_discharge_total": to_str(data.get('bdc1DischargeTotal')),
                "bdc1_fault_type": to_str(data.get('bdc1FaultType')),
                "bdc1_ibat": to_str(data.get('bdc1Ibat')),
                "bdc1_ibb": to_str(data.get('bdc1Ibb')),
                "bdc1_illc": to_str(data.get('bdc1Illc')),
                "bdc1_mode": to_str(data.get('bdc1Mode')),
                "bdc1_soc": to_str(data.get('bdc1Soc')),
                "bdc1_status": to_str(data.get('bdc1Status')),
                "bdc1_temp1": to_str(data.get('bdc1Temp1')),
                "bdc1_temp2": to_str(data.get('bdc1Temp2')),
                "bdc1_vbat": to_str(data.get('bdc1Vbat')),
                "bdc1_vbus1": to_str(data.get('bdc1Vbus1')),
                "bdc1_vbus2": to_str(data.get('bdc1Vbus2')),
                "bdc1_warn_code": to_str(data.get('bdc1WarnCode')),

                # BDC2 fields
                "bdc2_charge_power": to_str(data.get('bdc2ChargePower')),
                "bdc2_charge_total": to_str(data.get('bdc2ChargeTotal')),
                "bdc2_discharge_power": to_str(data.get('bdc2DischargePower')),
                "bdc2_discharge_total": to_str(data.get('bdc2DischargeTotal')),
                "bdc2_fault_type": to_str(data.get('bdc2FaultType')),
                "bdc2_ibat": to_str(data.get('bdc2Ibat')),
                "bdc2_ibb": to_str(data.get('bdc2Ibb')),
                "bdc2_illc": to_str(data.get('bdc2Illc')),
                "bdc2_mode": to_str(data.get('bdc2Mode')),
                "bdc2_soc": to_str(data.get('bdc2Soc')),
                "bdc2_status": to_str(data.get('bdc2Status')),
                "bdc2_temp1": to_str(data.get('bdc2Temp1')),
                "bdc2_temp2": to_str(data.get('bdc2Temp2')),
                "bdc2_vbat": to_str(data.get('bdc2Vbat')),
                "bdc2_vbus1": to_str(data.get('bdc2Vbus1')),
                "bdc2_vbus2": to_str(data.get('bdc2Vbus2')),
                "bdc2_warn_code": to_str(data.get('bdc2WarnCode')),

                # BDC common
                "bdc_bus_ref": to_str(data.get('bdcBusRef')),
                "bdc_derate_reason": to_str(data.get('bdcDerateReason')),
                "bdc_fault_sub_code": to_str(data.get('bdcFaultSubCode')),
                "bdc_status": to_str(data.get('bdcStatus')),
                "bdc_vbus2_neg": to_str(data.get('bdcVbus2Neg')),
                "bdc_warn_sub_code": to_str(data.get('bdcWarnSubCode')),

                # BMS fields
                "bms_communication_type": to_str(data.get('bmsCommunicationType')),
                "bms_cv_volt": to_str(data.get('bmsCvVolt')),
                "bms_error2": to_str(data.get('bmsError2')),
                "bms_error3": to_str(data.get('bmsError3')),
                "bms_error4": to_str(data.get('bmsError4')),
                "bms_fault_type": to_str(data.get('bmsFaultType')),
                "bms_fw_version": to_str(data.get('bmsFwVersion')),
                "bms_ibat": to_str(data.get('bmsIbat')),
                "bms_icycle": to_str(data.get('bmsIcycle')),
                "bms_info": to_str(data.get('bmsInfo')),
                "bms_ios_status": to_str(data.get('bmsIosStatus')),
                "bms_max_curr": to_str(data.get('bmsMaxCurr')),
                "bms_mcu_version": to_str(data.get('bmsMcuVersion')),
                "bms_pack_info": to_str(data.get('bmsPackInfo')),
                "bms_soc": to_str(data.get('bmsSoc')),
                "bms_soh": to_str(data.get('bmsSoh')),
                "bms_status": to_str(data.get('bmsStatus')),
                "bms_temp1_bat": to_str(data.get('bmsTemp1Bat')),
                "bms_using_cap": to_str(data.get('bmsUsingCap')),
                "bms_vbat": to_str(data.get('bmsVbat')),
                "bms_vdelta": to_str(data.get('bmsVdelta')),
                "bms_warn2": to_str(data.get('bmsWarn2')),
                "bms_warn_code": to_str(data.get('bmsWarnCode')),

                # Battery info
                "bat_sn": to_str(data.get('batSn')),
                "battery_no": to_str(data.get('batteryNo')),
                "battery_sn": to_str(data.get('batterySN')),
                "soc1": to_str(data.get('soc1')),
                "soc2": to_str(data.get('soc2')),

                # EPS fields
                "eps_fac": to_str(data.get('epsFac')),
                "eps_iac1": to_str(data.get('epsIac1')),
                "eps_iac2": to_str(data.get('epsIac2')),
                "eps_iac3": to_str(data.get('epsIac3')),
                "eps_pac": to_str(data.get('epsPac')),
                "eps_pac1": to_str(data.get('epsPac1')),
                "eps_pac2": to_str(data.get('epsPac2')),
                "eps_pac3": to_str(data.get('epsPac3')),
                "eps_pf": to_str(data.get('epsPf')),
                "eps_vac1": to_str(data.get('epsVac1')),
                "eps_vac2": to_str(data.get('epsVac2')),
                "eps_vac3": to_str(data.get('epsVac3')),

                # Fault and warning codes
                "fault_type": to_str(data.get('faultType')),
                "fault_type1": to_str(data.get('faultType1')),
                "warn_code": to_str(data.get('warnCode')),
                "warn_code1": to_str(data.get('warnCode1')),
                "warn_text": to_str(data.get('warnText')),
                "error_text": to_str(data.get('errorText')),
                "new_warn_code": to_str(data.get('newWarnCode')),
                "new_warn_sub_code": to_str(data.get('newWarnSubCode')),
                "sys_fault_word": to_str(data.get('sysFaultWord')),
                "sys_fault_word1": to_str(data.get('sysFaultWord1')),
                "sys_fault_word2": to_str(data.get('sysFaultWord2')),
                "sys_fault_word3": to_str(data.get('sysFaultWord3')),
                "sys_fault_word4": to_str(data.get('sysFaultWord4')),
                "sys_fault_word5": to_str(data.get('sysFaultWord5')),
                "sys_fault_word6": to_str(data.get('sysFaultWord6')),
                "sys_fault_word7": to_str(data.get('sysFaultWord7')),

                # Operating modes
                "operating_mode": to_str(data.get('operatingMode')),
                "derating_mode": to_str(data.get('deratingMode')),
                "bsystem_work_mode": to_str(data.get('bsystemWorkMode')),
                "uw_sys_work_mode": to_str(data.get('uwSysWorkMode')),
                "bgrid_type": to_str(data.get('bgridType')),

                # Other operational parameters
                "address": to_str(data.get('address')),
                "alias": to_str(data.get('alias')),
                "again": to_str(data.get('again')),
                "is_again": to_str(data.get('isAgain')),
                "b_merter_connect_flag": to_str(data.get('bMerterConnectFlag')),
                "day": to_str(data.get('day')),
                "dry_contact_status": to_str(data.get('dryContactStatus')),
                "gfci": to_str(data.get('gfci')),
                "inv_delay_time": to_str(data.get('invDelayTime')),
                "iso": to_str(data.get('iso')),
                "load_percent": to_str(data.get('loadPercent')),
                "lost": to_str(data.get('lost')),
                "mtnc_mode": to_str(data.get('mtncMode')),
                "mtnc_rqst": to_str(data.get('mtncRqst')),
                "op_fullwatt": to_str(data.get('opFullwatt')),
                "real_op_percent": to_str(data.get('realOPPercent')),
                "t_mtnc_strt": to_str(data.get('tMtncStrt')),
                "t_win_end": to_str(data.get('tWinEnd')),
                "t_win_start": to_str(data.get('tWinStart')),
                "time_total": to_str(data.get('timeTotal')),
                "total_working_time": to_str(data.get('totalWorkingTime')),
                "tlx_bean": to_str(data.get('tlxBean')),
                "win_mode": to_str(data.get('winMode')),
                "win_off_grid_soc": to_str(data.get('winOffGridSOC')),
                "win_on_grid_soc": to_str(data.get('winOnGridSOC')),
                "win_request": to_str(data.get('winRequest')),
                "with_time": to_str(data.get('withTime')),

                # Metadata
                "source_file": filename
            }

            all_rows.append(row)

        except Exception as e:
            print(f"      Error processing {filename}: {e}")
            continue

    # Create DataFrame from collected rows
    if all_rows:
        # Define nested schema for calendar struct
        calendar_gregorian_change_schema = StructType([
            StructField("date", StringType(), True),
            StructField("hours", StringType(), True),
            StructField("seconds", StringType(), True),
            StructField("month", StringType(), True),
            StructField("timezone_offset", StringType(), True),
            StructField("year", StringType(), True),
            StructField("minutes", StringType(), True),
            StructField("time", StringType(), True),
            StructField("day", StringType(), True)
        ])

        calendar_time_zone_schema = StructType([
            StructField("dirty", StringType(), True),
            StructField("last_rule_instance", StringType(), True),
            StructField("dst_savings", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("raw_offset", StringType(), True),
            StructField("id", StringType(), True)
        ])

        calendar_time_schema = StructType([
            StructField("date", StringType(), True),
            StructField("hours", StringType(), True),
            StructField("seconds", StringType(), True),
            StructField("month", StringType(), True),
            StructField("timezone_offset", StringType(), True),
            StructField("year", StringType(), True),
            StructField("minutes", StringType(), True),
            StructField("time", StringType(), True),
            StructField("day", StringType(), True)
        ])

        calendar_schema = StructType([
            StructField("calendar_type", StringType(), True),
            StructField("first_day_of_week", StringType(), True),
            StructField("minimal_days_in_first_week", StringType(), True),
            StructField("gregorian_change", calendar_gregorian_change_schema, True),
            StructField("time_zone", calendar_time_zone_schema, True),
            StructField("week_year", StringType(), True),
            StructField("time", calendar_time_schema, True),
            StructField("time_in_millis", StringType(), True),
            StructField("weeks_in_week_year", StringType(), True),
            StructField("lenient", StringType(), True),
            StructField("week_date_supported", StringType(), True)
        ])

        # Define main schema - all fields as strings except calendar struct
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("id_plant", StringType(), True),
            StructField("serial_num", StringType(), True),
            StructField("data_log_sn", StringType(), True),
            StructField("status", StringType(), True),
            StructField("status_text", StringType(), True),
            StructField("calendar", calendar_schema, True),
            StructField("pac", StringType(), True),
            StructField("pac1", StringType(), True),
            StructField("pac2", StringType(), True),
            StructField("pac3", StringType(), True),
            StructField("ppv", StringType(), True),
            StructField("ppv1", StringType(), True),
            StructField("ppv2", StringType(), True),
            StructField("ppv3", StringType(), True),
            StructField("ppv4", StringType(), True),
            StructField("eac_today", StringType(), True),
            StructField("eac_total", StringType(), True),
            StructField("epv1_today", StringType(), True),
            StructField("epv1_total", StringType(), True),
            StructField("epv2_today", StringType(), True),
            StructField("epv2_total", StringType(), True),
            StructField("epv3_today", StringType(), True),
            StructField("epv3_total", StringType(), True),
            StructField("epv4_today", StringType(), True),
            StructField("epv4_total", StringType(), True),
            StructField("epv_total", StringType(), True),
            StructField("eac_charge_today", StringType(), True),
            StructField("eac_charge_total", StringType(), True),
            StructField("echarge_today", StringType(), True),
            StructField("echarge_total", StringType(), True),
            StructField("edischarge_today", StringType(), True),
            StructField("edischarge_total", StringType(), True),
            StructField("eto_grid_today", StringType(), True),
            StructField("eto_grid_total", StringType(), True),
            StructField("eto_user_today", StringType(), True),
            StructField("eto_user_total", StringType(), True),
            StructField("elocal_load_today", StringType(), True),
            StructField("elocal_load_total", StringType(), True),
            StructField("eself_today", StringType(), True),
            StructField("eself_total", StringType(), True),
            StructField("esystem_today", StringType(), True),
            StructField("esystem_total", StringType(), True),
            StructField("eex1_today", StringType(), True),
            StructField("eex1_total", StringType(), True),
            StructField("eex2_today", StringType(), True),
            StructField("eex2_total", StringType(), True),
            StructField("pac_to_grid_total", StringType(), True),
            StructField("pac_to_local_load", StringType(), True),
            StructField("pac_to_user_total", StringType(), True),
            StructField("pself", StringType(), True),
            StructField("psystem", StringType(), True),
            StructField("pex1", StringType(), True),
            StructField("pex2", StringType(), True),
            StructField("pacr", StringType(), True),
            StructField("vac1", StringType(), True),
            StructField("vac2", StringType(), True),
            StructField("vac3", StringType(), True),
            StructField("vac_rs", StringType(), True),
            StructField("vac_st", StringType(), True),
            StructField("vac_tr", StringType(), True),
            StructField("vacr", StringType(), True),
            StructField("vacrs", StringType(), True),
            StructField("vpv1", StringType(), True),
            StructField("vpv2", StringType(), True),
            StructField("vpv3", StringType(), True),
            StructField("vpv4", StringType(), True),
            StructField("dc_voltage", StringType(), True),
            StructField("p_bus_voltage", StringType(), True),
            StructField("n_bus_voltage", StringType(), True),
            StructField("iac1", StringType(), True),
            StructField("iac2", StringType(), True),
            StructField("iac3", StringType(), True),
            StructField("iacr", StringType(), True),
            StructField("ipv1", StringType(), True),
            StructField("ipv2", StringType(), True),
            StructField("ipv3", StringType(), True),
            StructField("ipv4", StringType(), True),
            StructField("dci_r", StringType(), True),
            StructField("dci_s", StringType(), True),
            StructField("dci_t", StringType(), True),
            StructField("fac", StringType(), True),
            StructField("pf", StringType(), True),
            StructField("temp1", StringType(), True),
            StructField("temp2", StringType(), True),
            StructField("temp3", StringType(), True),
            StructField("temp4", StringType(), True),
            StructField("temp5", StringType(), True),
            StructField("bdc1_charge_power", StringType(), True),
            StructField("bdc1_charge_total", StringType(), True),
            StructField("bdc1_discharge_power", StringType(), True),
            StructField("bdc1_discharge_total", StringType(), True),
            StructField("bdc1_fault_type", StringType(), True),
            StructField("bdc1_ibat", StringType(), True),
            StructField("bdc1_ibb", StringType(), True),
            StructField("bdc1_illc", StringType(), True),
            StructField("bdc1_mode", StringType(), True),
            StructField("bdc1_soc", StringType(), True),
            StructField("bdc1_status", StringType(), True),
            StructField("bdc1_temp1", StringType(), True),
            StructField("bdc1_temp2", StringType(), True),
            StructField("bdc1_vbat", StringType(), True),
            StructField("bdc1_vbus1", StringType(), True),
            StructField("bdc1_vbus2", StringType(), True),
            StructField("bdc1_warn_code", StringType(), True),
            StructField("bdc2_charge_power", StringType(), True),
            StructField("bdc2_charge_total", StringType(), True),
            StructField("bdc2_discharge_power", StringType(), True),
            StructField("bdc2_discharge_total", StringType(), True),
            StructField("bdc2_fault_type", StringType(), True),
            StructField("bdc2_ibat", StringType(), True),
            StructField("bdc2_ibb", StringType(), True),
            StructField("bdc2_illc", StringType(), True),
            StructField("bdc2_mode", StringType(), True),
            StructField("bdc2_soc", StringType(), True),
            StructField("bdc2_status", StringType(), True),
            StructField("bdc2_temp1", StringType(), True),
            StructField("bdc2_temp2", StringType(), True),
            StructField("bdc2_vbat", StringType(), True),
            StructField("bdc2_vbus1", StringType(), True),
            StructField("bdc2_vbus2", StringType(), True),
            StructField("bdc2_warn_code", StringType(), True),
            StructField("bdc_bus_ref", StringType(), True),
            StructField("bdc_derate_reason", StringType(), True),
            StructField("bdc_fault_sub_code", StringType(), True),
            StructField("bdc_status", StringType(), True),
            StructField("bdc_vbus2_neg", StringType(), True),
            StructField("bdc_warn_sub_code", StringType(), True),
            StructField("bms_communication_type", StringType(), True),
            StructField("bms_cv_volt", StringType(), True),
            StructField("bms_error2", StringType(), True),
            StructField("bms_error3", StringType(), True),
            StructField("bms_error4", StringType(), True),
            StructField("bms_fault_type", StringType(), True),
            StructField("bms_fw_version", StringType(), True),
            StructField("bms_ibat", StringType(), True),
            StructField("bms_icycle", StringType(), True),
            StructField("bms_info", StringType(), True),
            StructField("bms_ios_status", StringType(), True),
            StructField("bms_max_curr", StringType(), True),
            StructField("bms_mcu_version", StringType(), True),
            StructField("bms_pack_info", StringType(), True),
            StructField("bms_soc", StringType(), True),
            StructField("bms_soh", StringType(), True),
            StructField("bms_status", StringType(), True),
            StructField("bms_temp1_bat", StringType(), True),
            StructField("bms_using_cap", StringType(), True),
            StructField("bms_vbat", StringType(), True),
            StructField("bms_vdelta", StringType(), True),
            StructField("bms_warn2", StringType(), True),
            StructField("bms_warn_code", StringType(), True),
            StructField("bat_sn", StringType(), True),
            StructField("battery_no", StringType(), True),
            StructField("battery_sn", StringType(), True),
            StructField("soc1", StringType(), True),
            StructField("soc2", StringType(), True),
            StructField("eps_fac", StringType(), True),
            StructField("eps_iac1", StringType(), True),
            StructField("eps_iac2", StringType(), True),
            StructField("eps_iac3", StringType(), True),
            StructField("eps_pac", StringType(), True),
            StructField("eps_pac1", StringType(), True),
            StructField("eps_pac2", StringType(), True),
            StructField("eps_pac3", StringType(), True),
            StructField("eps_pf", StringType(), True),
            StructField("eps_vac1", StringType(), True),
            StructField("eps_vac2", StringType(), True),
            StructField("eps_vac3", StringType(), True),
            StructField("fault_type", StringType(), True),
            StructField("fault_type1", StringType(), True),
            StructField("warn_code", StringType(), True),
            StructField("warn_code1", StringType(), True),
            StructField("warn_text", StringType(), True),
            StructField("error_text", StringType(), True),
            StructField("new_warn_code", StringType(), True),
            StructField("new_warn_sub_code", StringType(), True),
            StructField("sys_fault_word", StringType(), True),
            StructField("sys_fault_word1", StringType(), True),
            StructField("sys_fault_word2", StringType(), True),
            StructField("sys_fault_word3", StringType(), True),
            StructField("sys_fault_word4", StringType(), True),
            StructField("sys_fault_word5", StringType(), True),
            StructField("sys_fault_word6", StringType(), True),
            StructField("sys_fault_word7", StringType(), True),
            StructField("operating_mode", StringType(), True),
            StructField("derating_mode", StringType(), True),
            StructField("bsystem_work_mode", StringType(), True),
            StructField("uw_sys_work_mode", StringType(), True),
            StructField("bgrid_type", StringType(), True),
            StructField("address", StringType(), True),
            StructField("alias", StringType(), True),
            StructField("again", StringType(), True),
            StructField("is_again", StringType(), True),
            StructField("b_merter_connect_flag", StringType(), True),
            StructField("day", StringType(), True),
            StructField("dry_contact_status", StringType(), True),
            StructField("gfci", StringType(), True),
            StructField("inv_delay_time", StringType(), True),
            StructField("iso", StringType(), True),
            StructField("load_percent", StringType(), True),
            StructField("lost", StringType(), True),
            StructField("mtnc_mode", StringType(), True),
            StructField("mtnc_rqst", StringType(), True),
            StructField("op_fullwatt", StringType(), True),
            StructField("real_op_percent", StringType(), True),
            StructField("t_mtnc_strt", StringType(), True),
            StructField("t_win_end", StringType(), True),
            StructField("t_win_start", StringType(), True),
            StructField("time_total", StringType(), True),
            StructField("total_working_time", StringType(), True),
            StructField("tlx_bean", StringType(), True),
            StructField("win_mode", StringType(), True),
            StructField("win_off_grid_soc", StringType(), True),
            StructField("win_on_grid_soc", StringType(), True),
            StructField("win_request", StringType(), True),
            StructField("with_time", StringType(), True),
            StructField("source_file", StringType(), True)
        ])

        # Helper function to convert calendar dict to proper struct format
        def format_calendar(cal_data):
            if not cal_data:
                return None
            return {
                "calendar_type": str(cal_data.get('calendarType')) if cal_data.get('calendarType') is not None else None,
                "first_day_of_week": str(cal_data.get('firstDayOfWeek')) if cal_data.get('firstDayOfWeek') is not None else None,
                "minimal_days_in_first_week": str(cal_data.get('minimalDaysInFirstWeek')) if cal_data.get('minimalDaysInFirstWeek') is not None else None,
                "gregorian_change": {
                    "date": str(cal_data.get('gregorianChange', {}).get('date')) if cal_data.get('gregorianChange', {}).get('date') is not None else None,
                    "hours": str(cal_data.get('gregorianChange', {}).get('hours')) if cal_data.get('gregorianChange', {}).get('hours') is not None else None,
                    "seconds": str(cal_data.get('gregorianChange', {}).get('seconds')) if cal_data.get('gregorianChange', {}).get('seconds') is not None else None,
                    "month": str(cal_data.get('gregorianChange', {}).get('month')) if cal_data.get('gregorianChange', {}).get('month') is not None else None,
                    "timezone_offset": str(cal_data.get('gregorianChange', {}).get('timezoneOffset')) if cal_data.get('gregorianChange', {}).get('timezoneOffset') is not None else None,
                    "year": str(cal_data.get('gregorianChange', {}).get('year')) if cal_data.get('gregorianChange', {}).get('year') is not None else None,
                    "minutes": str(cal_data.get('gregorianChange', {}).get('minutes')) if cal_data.get('gregorianChange', {}).get('minutes') is not None else None,
                    "time": str(cal_data.get('gregorianChange', {}).get('time')) if cal_data.get('gregorianChange', {}).get('time') is not None else None,
                    "day": str(cal_data.get('gregorianChange', {}).get('day')) if cal_data.get('gregorianChange', {}).get('day') is not None else None
                } if cal_data.get('gregorianChange') else None,
                "time_zone": {
                    "dirty": str(cal_data.get('timeZone', {}).get('dirty')) if cal_data.get('timeZone', {}).get('dirty') is not None else None,
                    "last_rule_instance": str(cal_data.get('timeZone', {}).get('lastRuleInstance')) if cal_data.get('timeZone', {}).get('lastRuleInstance') is not None else None,
                    "dst_savings": str(cal_data.get('timeZone', {}).get('DSTSavings')) if cal_data.get('timeZone', {}).get('DSTSavings') is not None else None,
                    "display_name": str(cal_data.get('timeZone', {}).get('displayName')) if cal_data.get('timeZone', {}).get('displayName') is not None else None,
                    "raw_offset": str(cal_data.get('timeZone', {}).get('rawOffset')) if cal_data.get('timeZone', {}).get('rawOffset') is not None else None,
                    "id": str(cal_data.get('timeZone', {}).get('ID')) if cal_data.get('timeZone', {}).get('ID') is not None else None
                } if cal_data.get('timeZone') else None,
                "week_year": str(cal_data.get('weekYear')) if cal_data.get('weekYear') is not None else None,
                "time": {
                    "date": str(cal_data.get('time', {}).get('date')) if cal_data.get('time', {}).get('date') is not None else None,
                    "hours": str(cal_data.get('time', {}).get('hours')) if cal_data.get('time', {}).get('hours') is not None else None,
                    "seconds": str(cal_data.get('time', {}).get('seconds')) if cal_data.get('time', {}).get('seconds') is not None else None,
                    "month": str(cal_data.get('time', {}).get('month')) if cal_data.get('time', {}).get('month') is not None else None,
                    "timezone_offset": str(cal_data.get('time', {}).get('timezoneOffset')) if cal_data.get('time', {}).get('timezoneOffset') is not None else None,
                    "year": str(cal_data.get('time', {}).get('year')) if cal_data.get('time', {}).get('year') is not None else None,
                    "minutes": str(cal_data.get('time', {}).get('minutes')) if cal_data.get('time', {}).get('minutes') is not None else None,
                    "time": str(cal_data.get('time', {}).get('time')) if cal_data.get('time', {}).get('time') is not None else None,
                    "day": str(cal_data.get('time', {}).get('day')) if cal_data.get('time', {}).get('day') is not None else None
                } if cal_data.get('time') else None,
                "time_in_millis": str(cal_data.get('timeInMillis')) if cal_data.get('timeInMillis') is not None else None,
                "weeks_in_week_year": str(cal_data.get('weeksInWeekYear')) if cal_data.get('weeksInWeekYear') is not None else None,
                "lenient": str(cal_data.get('lenient')) if cal_data.get('lenient') is not None else None,
                "week_date_supported": str(cal_data.get('weekDateSupported')) if cal_data.get('weekDateSupported') is not None else None
            }

        # Reprocess all_rows to properly format calendar field
        formatted_rows = []
        for row in all_rows:
            row_copy = row.copy()
            row_copy['calendar'] = format_calendar(row.get('calendar'))
            formatted_rows.append(row_copy)

        # Create DataFrame with explicit schema
        df = spark.createDataFrame(formatted_rows, schema)

        # Convert timestamp string to timestamp type and add processing timestamp
        df = (
            df
            .withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("bronze_processing_timestamp", F.current_timestamp())
        )

        # Write to device-specific bronze table
        sanitized_sn = device_sn.replace('-', '_').replace(' ', '_').lower()
        table_path = f"{CATALOG_NAME}.bronze.solarflow_{sanitized_sn}_batch"
        df.write.mode("append").saveAsTable(table_path)

        print(f"    Plant {plant_id} Device {device_sn}: Wrote {len(all_rows)} rows to {table_path}")

# COMMAND ----------

# DBTITLE 1,Main Processing Function
def process_solarflow_main_batch(batch_df, batch_id):
    """
    Process each batch of JSON files from Autoloader.
    Extracts plant_id and device_sn from filename.
    Creates appropriate bronze tables and processes each combination separately.
    """
    # Add metadata columns extracted from filename
    # Expected pattern: solarflow_{plant_id}_{device_sn}_{timestamp}.json
    batch_with_metadata = batch_df.withColumn(
        "filename",
        F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1)
    ).withColumn(
        "plant_id",
        F.regexp_extract(F.col("filename"), r"solarflow_(\d+)_[A-Za-z0-9\-]+_\d{8}_\d{6}\.json", 1)
    ).withColumn(
        "device_sn",
        F.regexp_extract(F.col("filename"), r"solarflow_\d+_([A-Za-z0-9\-]+)_\d{8}_\d{6}\.json", 1)
    )

    print(f"Batch {batch_id}: Starting processing...")

    # Get unique plant_id + device_sn combinations from filenames
    plant_device_combinations = [
        {"plant_id": row['plant_id'], "device_sn": row['device_sn']}
        for row in batch_with_metadata.select("plant_id", "device_sn").distinct().collect()
        if row['plant_id'] and row['device_sn']
    ]

    if not plant_device_combinations:
        print(f"Batch {batch_id}: ✗ No valid plant/device combinations found from filenames - cannot create tables.")
        return

    print(f"Batch {batch_id}: Found {len(plant_device_combinations)} plant/device combination(s).")
    for combo in plant_device_combinations:
        print(f"  - Plant {combo['plant_id']}, Device {combo['device_sn']}")

    # Create bronze tables for each combination
    for combo in plant_device_combinations:
        plant_id = combo['plant_id']
        device_sn = combo['device_sn']
        create_bronze_table(CATALOG_NAME, plant_id, device_sn)

    # Process each combination separately
    for combo in plant_device_combinations:
        plant_id = combo['plant_id']
        device_sn = combo['device_sn']
        process_solarflow_batch(batch_with_metadata, batch_id, plant_id, device_sn)

    print(f"Batch {batch_id}: Processing complete.")

# COMMAND ----------

# DBTITLE 1,Read JSON Files with Autoloader
# Read as binary to get file paths, then process each file individually
stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE_PATH}solarflow_raw_batch/")
        .option("pathGlobFilter", "*.json")
        .load(VOLUME_PATH)
)

# COMMAND ----------

# DBTITLE 1,Process Stream with foreachBatch
query = (
    stream_df
    .writeStream
    .foreachBatch(process_solarflow_main_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}solarflow_raw_batch/")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# DBTITLE 1,Wait for Stream to Complete
query.awaitTermination()