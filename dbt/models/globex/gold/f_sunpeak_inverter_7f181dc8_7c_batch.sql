WITH inverter_raw AS (
  SELECT
    timestamp,
    id_site,
    serial_number,

    -- Fields for Haystack mapping
    ac_power,       -- Total AC power
    ac_current_l1,            -- AC current
    ac_energy_production,            -- Cumulative energy
    dc_voltage,              -- DC voltage
    ac_frequency,          -- Frequency
    ac_power_factor,               -- Power factor
    ac_apparent_power,        -- Apparent power
    ac_reactive_power,        -- Reactive power
    ac_voltage_l1,            -- AC voltage
    temperature,            -- Temperature
    operating_state            -- Operating state

  FROM {{ source('globex', 'sunpeak_7f181dc8_7c_batch') }}
  {% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) - INTERVAL 60 MINUTES FROM {{ this }})
  {% endif %}
),
-- Join with dimension table to get surrogate keys and keep id_site for final output
inverter_with_keys AS (
  SELECT
    r.timestamp,
    r.id_site,
    r.serial_number,
    d.sk_inverter AS sk_inverter,
    d.sk_site AS sk_site,
    d.sk_space AS sk_space,
    r.ac_power,
    r.ac_current_l1,
    r.ac_energy_production,
    r.dc_voltage,
    r.ac_frequency,
    r.ac_power_factor,
    r.ac_apparent_power,
    r.ac_reactive_power,
    r.ac_voltage_l1,
    r.temperature,
    r.operating_state
  FROM inverter_raw r
  INNER JOIN {{ source('globex_gold', 'd_inverters') }} d
    ON r.id_site = d.id_site
    AND r.serial_number = d.serial_number
    AND d.connector = 'sunpeak_api'
),
-- Floor timestamps to 15-minute boundaries for consistent aggregation
-- Example: 08:17:15 -> 08:15:00, 08:22:15 -> 08:15:00, 08:32:15 -> 08:30:00
inverter_floored AS (
  SELECT
    id_site,
    serial_number,
    sk_inverter,
    sk_site,
    sk_space,
    -- Floor timestamp to nearest 15 minutes
    TIMESTAMP_SECONDS(
      CAST(UNIX_TIMESTAMP(timestamp) / 900 AS BIGINT) * 900
    ) AS timestamp_floored,
    ac_power,
    ac_current_l1,
    ac_energy_production,
    dc_voltage,
    ac_frequency,
    ac_power_factor,
    ac_apparent_power,
    ac_reactive_power,
    ac_voltage_l1,
    temperature,
    operating_state
  FROM inverter_with_keys
),
-- Aggregate inverter data from 5-minute to 15-minute intervals
inverter_aggregated AS (
  SELECT
    -- Use FIRST() to get id_site and serial_number since they're constant per sk_inverter
    FIRST(id_site) AS id_site,
    FIRST(serial_number) AS serial_number,
    sk_inverter,
    sk_site,
    sk_space,
    timestamp_floored AS timestamp,
    CAST(AVG(ac_power) AS DECIMAL(15,2)) AS ac_power,
    CAST(AVG(ac_current_l1) AS DECIMAL(10,2)) AS ac_current_l1,
    CAST(MAX(ac_energy_production) AS DECIMAL(20,2)) AS ac_energy_production,
    CAST(AVG(dc_voltage) AS DECIMAL(10,2)) AS dc_voltage,
    CAST(AVG(ac_frequency) AS DECIMAL(10,2)) AS ac_frequency,
    CAST(AVG(ac_power_factor) AS DECIMAL(10,3)) AS ac_power_factor,
    CAST(AVG(ac_apparent_power) AS DECIMAL(15,2)) AS ac_apparent_power,
    CAST(AVG(ac_reactive_power) AS DECIMAL(15,2)) AS ac_reactive_power,
    CAST(AVG(ac_voltage_l1) AS DECIMAL(10,2)) AS ac_voltage_l1,
    CAST(AVG(temperature) AS DECIMAL(10,2)) AS temperature,
    -- Take the most common mode in the interval (or LAST if all unique)
    LAST(operating_state) AS operating_state
  FROM inverter_floored
  GROUP BY
    sk_inverter,
    sk_site,
    sk_space,
    timestamp_floored
),
-- Calculate interval energy from cumulative ac_energy_production using LAG window function
inverter_with_interval_energy AS (
  SELECT
    id_site,
    serial_number,
    sk_inverter,
    sk_site,
    sk_space,
    timestamp,
    ac_power,
    ac_current_l1,
    ac_energy_production,
    dc_voltage,
    ac_frequency,
    ac_power_factor,
    ac_apparent_power,
    ac_reactive_power,
    ac_voltage_l1,
    temperature,
    operating_state,
    -- Calculate interval energy: current cumulative - previous cumulative
    CASE
      WHEN LAG(ac_energy_production) OVER (PARTITION BY sk_inverter ORDER BY timestamp) IS NOT NULL
      THEN CAST(ac_energy_production - LAG(ac_energy_production) OVER (PARTITION BY sk_inverter ORDER BY timestamp) AS DECIMAL(20,2))
      ELSE NULL
    END AS interval_energy_wh
  FROM inverter_aggregated
),
haystack_telemetry AS (
  SELECT
    timestamp,
    sk_inverter,
    sk_site,
    sk_space,

    -- ac_power: AC Power (Watts)
    CAST(ac_power AS DECIMAL(10,2)) AS ac_power,

    -- ac_current_l1: AC Current Phase A (Amps)
    CAST(ac_current_l1 AS DECIMAL(10,2)) AS ac_current_l1,

    -- ac_energy_production: AC Energy (Watt-hours)
    CAST(interval_energy_wh AS DECIMAL(10,2)) AS ac_energy_production,

    -- dc_voltage: DC Voltage (Volts)
    CAST(dc_voltage AS DECIMAL(10,2)) AS dc_voltage,

    -- ac_frequency: Line Frequency (Hertz)
    CAST(ac_frequency AS DECIMAL(10,2)) AS ac_frequency,

    -- ac_power_factor: AC Power Factor (dimensionless)
    CAST(ac_power_factor AS DECIMAL(10,3)) AS ac_power_factor,

    -- ac_apparent_power: AC Apparent Power (Volt-ampere)
    CAST(ac_apparent_power AS DECIMAL(10,2)) AS ac_apparent_power,

    -- ac_reactive_power: AC Reactive Power (Volt-ampere reactive)
    CAST(ac_reactive_power AS DECIMAL(10,2)) AS ac_reactive_power,

    -- ac_voltage_l1: Phase-to-neutral Voltage Phase A (Volts)
    CAST(ac_voltage_l1 AS DECIMAL(10,2)) AS ac_voltage_l1,

    -- temperature_other: Other Temperature (Celsius)
    CAST(temperature AS DECIMAL(10,2)) AS temperature_other,

    -- operating_state: Operating State (Enum)
    CASE
      WHEN operating_state = 'SLEEPING' THEN '2'
      WHEN operating_state = 'STARTING' THEN '3'
      WHEN operating_state = 'MPPT' THEN '4'
      WHEN operating_state = 'THROTTLED' THEN '5'
      WHEN operating_state = 'SHUTTING_DOWN' THEN '6'
      WHEN operating_state = 'ERROR' THEN '7'  -- FAULT
      WHEN operating_state = 'STANDBY' THEN '8'
      ELSE 'OFF'
    END AS operating_state

  FROM inverter_with_interval_energy
)

SELECT * FROM haystack_telemetry
{% if is_incremental() %}
  WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}
ORDER BY timestamp
