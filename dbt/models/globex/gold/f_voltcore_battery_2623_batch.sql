WITH silver_data AS (
  SELECT
    timestamp,
    id_site,
    state_of_charge,
    dc_voltage,
    dc_current,
    dc_voltage_max,
    dc_voltage_min,
    charge_cycles,
    energy_charge,
    energy_discharge,
    battery_state
  FROM {{ source('globex', 'voltcore_2623_batch') }}
  {% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) - INTERVAL 60 MINUTES FROM {{ this }})
  {% endif %}
),
-- Join with dimension table to get surrogate keys
-- Note: Device instance 2623 maps to battery monitor for MultiPlus-II
silver_with_keys AS (
  SELECT
    s.timestamp,
    d.sk_battery AS sk_battery,
    d.sk_site AS sk_site,
    d.sk_space AS sk_space,
    s.state_of_charge,
    s.dc_voltage,
    s.dc_current,
    s.dc_voltage_max,
    s.dc_voltage_min,
    s.charge_cycles,
    s.energy_charge,
    s.energy_discharge,
    s.battery_state
  FROM silver_data s
  INNER JOIN {{ source('globex_gold', 'd_batteries') }} d
    ON s.id_site = d.id_site
    AND d.connector = 'voltcore_api'
    AND d.device_instance = 'a3e5'  -- Battery product code for site voltcore-37151
),
with_power_energy AS (
  SELECT
    timestamp,
    sk_battery,
    sk_site,
    sk_space,
    state_of_charge,
    dc_voltage,
    dc_current,
    dc_voltage_max,
    dc_voltage_min,
    charge_cycles,
    energy_charge,
    energy_discharge,
    battery_state,
    -- Calculate instantaneous power (dc_power = dc_voltage * dc_current)
    CAST(dc_voltage * dc_current AS DECIMAL(10,2)) AS power_w,
    -- Calculate rolling hourly energy in Watt-hours
    SUM(dc_voltage * dc_current) OVER (
      PARTITION BY sk_battery
      ORDER BY timestamp
      RANGE BETWEEN INTERVAL 59 MINUTES PRECEDING AND CURRENT ROW
    ) / 60 AS energy_wh_hourly
  FROM silver_with_keys
  WHERE dc_voltage IS NOT NULL AND dc_current IS NOT NULL
),
windowed_data AS (
  SELECT
    FIRST(sk_battery) AS sk_battery,
    FIRST(sk_site) AS sk_site,
    FIRST(sk_space) AS sk_space,
    WINDOW(timestamp, '15 minutes').start AS timestamp,
    CAST(AVG(state_of_charge) AS DECIMAL(10,2)) AS state_of_charge,
    CAST(AVG(dc_voltage) AS DECIMAL(10,2)) AS dc_voltage,
    CAST(AVG(dc_current) AS DECIMAL(10,2)) AS dc_current,
    CAST(AVG(dc_voltage_max) AS DECIMAL(10,2)) AS dc_voltage_max,
    CAST(AVG(dc_voltage_min) AS DECIMAL(10,2)) AS dc_voltage_min,
    LAST(charge_cycles) AS charge_cycles,
    LAST(energy_charge) AS energy_charge,
    LAST(energy_discharge) AS energy_discharge,
    LAST(battery_state) AS battery_state,
    CAST(AVG(power_w) AS DECIMAL(10,2)) AS power_w,
    CAST(LAST(energy_wh_hourly) AS DECIMAL(10,2)) AS energy_wh
  FROM with_power_energy
  GROUP BY
    WINDOW(timestamp, '15 minutes'),
    sk_battery,
    sk_site,
    sk_space
),
haystack_telemetry AS (
  SELECT
    timestamp,
    sk_battery,
    sk_site,
    sk_space,

    -- state_of_charge: operating_state of Charge (Percentage)
    CAST(state_of_charge AS DECIMAL(10,2)) AS state_of_charge,

    -- dc_voltage: DC Bus Voltage (Volts)
    CAST(dc_voltage AS DECIMAL(10,2)) AS dc_voltage,

    -- dc_current: Total DC Current (Amps) - positive = charging, negative = discharging
    CAST(dc_current AS DECIMAL(10,2)) AS dc_current,

    -- dc_power: Total Power (Watts) - calculated from dc_voltage * dc_current
    CAST(power_w AS DECIMAL(10,2)) AS dc_power,

    -- energy_hourly: Energy (Watt-hours) - rolling hourly energy
    CAST(energy_wh AS DECIMAL(10,2)) AS energy_hourly,

    -- dc_voltage_max: Max Battery Voltage (Volts) - runtime telemetry
    CAST(dc_voltage_max AS DECIMAL(10,2)) AS dc_voltage_max,

    -- dc_voltage_min: Min Battery Voltage (Volts) - runtime telemetry
    CAST(dc_voltage_min AS DECIMAL(10,2)) AS dc_voltage_min,

    -- charge_cycles: Charge Cycles - number of charge cycles executed
    charge_cycles AS charge_cycles,

    -- operating_state: Battery operating_state (Running, Standby, Fault, etc.)
    battery_state AS operating_state,

    -- Additional energy metrics (not in base ontology but valuable for analysis)
    CAST(energy_charge AS DECIMAL(10,2)) AS energy_charge,
    CAST(energy_discharge AS DECIMAL(10,2)) AS energy_discharge

  FROM windowed_data
)

SELECT * FROM haystack_telemetry
{% if is_incremental() %}
  WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}
ORDER BY timestamp
