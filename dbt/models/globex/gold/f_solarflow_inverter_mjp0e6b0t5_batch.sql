WITH unified_silver AS (
  SELECT
    timestamp,
    id_site,

    -- Fields for Haystack mapping
    ac_power,                     -- Total AC power
    ac_current_l1, ac_current_l2, ac_current_l3,        -- AC current per phase
    energy_production_daily,                -- Daily energy (kWh → convert to Wh)
    dc_power,                     -- DC power
    ac_frequency,                     -- Frequency
    ac_power_factor,                      -- Power factor
    ac_voltage_l1_l2, ac_voltage_l2_l3, ac_voltage_l3_l1,     -- Phase-to-phase voltages
    ac_voltage_l1, ac_voltage_l2, ac_voltage_l3,        -- Phase-to-neutral voltages
    temp1, temp2, temp5,     -- Temperatures
    operating_state                   -- Operating operating_state

  FROM {{ source('globex', 'solarflow_mjp0e6b0t5_batch') }}
  {% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) - INTERVAL 60 MINUTES FROM {{ this }})
  {% endif %}
),
-- Join with dimension table to get surrogate keys
-- Note: Solarflow silver tables are per-device, so we join only on id_site and serial number 'mjp0e6b0t5'
silver_with_keys AS (
  SELECT
    s.timestamp,
    d.sk_inverter AS sk_inverter,
    d.sk_site AS sk_site,
    d.sk_space AS sk_space,
    s.ac_power,
    s.ac_current_l1,
    s.ac_current_l2,
    s.ac_current_l3,
    s.energy_production_daily,
    s.dc_power,
    s.ac_frequency,
    s.ac_power_factor,
    s.ac_voltage_l1_l2,
    s.ac_voltage_l2_l3,
    s.ac_voltage_l3_l1,
    s.ac_voltage_l1,
    s.ac_voltage_l2,
    s.ac_voltage_l3,
    s.temp1,
    s.temp2,
    s.temp5,
    s.operating_state
  FROM unified_silver s
  INNER JOIN {{ source('globex_gold', 'd_inverters') }} d
    ON s.id_site = d.id_site
    AND UPPER(d.serial_number) = 'MJP0E6B0T5'
    AND d.connector = 'solarflow_api'
),
haystack_telemetry AS (
  SELECT
    timestamp,
    sk_inverter,
    sk_site,
    sk_space,

    -- ac_power - AC Power (Watts)
    CAST(ac_power AS DECIMAL(10,2)) AS ac_power,

    -- ac_current_l1, ac_current_l2, ac_current_l3 - AC Current per phase (Amps)
    CAST(ac_current_l1 AS DECIMAL(10,2)) AS ac_current_l1,
    CAST(ac_current_l2 AS DECIMAL(10,2)) AS ac_current_l2,
    CAST(ac_current_l3 AS DECIMAL(10,2)) AS ac_current_l3,

    -- ac_energy_production - AC Energy (Watt-hours) - CONVERTED FROM kWh
    CAST(energy_production_daily * 1000 AS DECIMAL(10,2)) AS ac_energy_production,

    -- dc_power - DC Power (Watts)
    CAST(dc_power AS DECIMAL(10,2)) AS dc_power,

    -- ac_frequency - Line Frequency (Hertz)
    CAST(ac_frequency AS DECIMAL(10,2)) AS ac_frequency,

    -- ac_power_factor - AC Power Factor (dimensionless)
    CAST(ac_power_factor AS DECIMAL(10,3)) AS ac_power_factor,

    -- ac_voltage_l1_l2, ac_voltage_l2_l3, ac_voltage_l3_l1 - Phase-to-phase voltages (Volts)
    CAST(ac_voltage_l1_l2 AS DECIMAL(10,2)) AS ac_voltage_l1_l2,
    CAST(ac_voltage_l2_l3 AS DECIMAL(10,2)) AS ac_voltage_l2_l3,
    CAST(ac_voltage_l3_l1 AS DECIMAL(10,2)) AS ac_voltage_l3_l1,

    -- ac_voltage_l1, ac_voltage_l2, ac_voltage_l3 - Phase-to-neutral voltages (Volts)
    CAST(ac_voltage_l1 AS DECIMAL(10,2)) AS ac_voltage_l1,
    CAST(ac_voltage_l2 AS DECIMAL(10,2)) AS ac_voltage_l2,
    CAST(ac_voltage_l3 AS DECIMAL(10,2)) AS ac_voltage_l3,

    -- temperature_other - Temperature (Celsius)
    CAST(temp5 AS DECIMAL(10,2)) AS temperature_other,

    -- operating_state: Operating State (Enum)
    CASE
      WHEN operating_state = '8' THEN 'STANDBY' -- Waiting
      WHEN operating_state = '4' THEN 'MPPT'
      WHEN operating_state = '7' THEN 'FAULT'
      ELSE 'OFF'
    END AS operating_state

  FROM silver_with_keys
)

SELECT * FROM haystack_telemetry
{% if is_incremental() %}
  WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}
ORDER BY timestamp
