WITH silver_data AS (
  SELECT
    timestamp,
    id_site,
    input_voltage,
    input_current,
    input_frequency,
    input_power,
    ac_voltage_l1,
    ac_current_l1,
    ac_frequency,
    ac_power,
    charge_state,
    temperature_cabinet,
    temperature_ambient
  FROM {{ source('globex', 'voltcore_2611_batch') }}
  {% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) - INTERVAL 60 MINUTES FROM {{ this }})
  {% endif %}
),
-- Join with dimension table to get surrogate keys
-- Note: For Voltcore, device_instance 2611 maps to VE.Bus System inverters
silver_with_keys AS (
  SELECT
    s.timestamp,
    d.sk_inverter AS sk_inverter,
    d.sk_site AS sk_site,
    d.sk_space AS sk_space,
    s.input_voltage,
    s.input_current,
    s.input_frequency,
    s.input_power,
    s.ac_voltage_l1,
    s.ac_current_l1,
    s.ac_frequency,
    s.ac_power,
    s.charge_state,
    s.temperature_cabinet,
    s.temperature_ambient
  FROM silver_data s
  INNER JOIN {{ source('globex_gold', 'd_inverters') }} d
    ON s.id_site = d.id_site
    AND d.connector = 'voltcore_api'
    AND d.device_instance = '2611'
),
with_hourly_energy AS (
  SELECT
    timestamp,
    sk_inverter,
    sk_site,
    sk_space,
    input_voltage,
    input_current,
    input_frequency,
    input_power,
    ac_voltage_l1,
    ac_current_l1,
    ac_frequency,
    ac_power,
    charge_state,
    -- Calculate rolling hourly energy in Watt-hours from output power
    SUM(ac_power) OVER (
      PARTITION BY sk_inverter
      ORDER BY timestamp
      RANGE BETWEEN INTERVAL 59 MINUTES PRECEDING AND CURRENT ROW
    ) / 60 AS energy_wh_hourly,
    temperature_cabinet,
    temperature_ambient
  FROM silver_with_keys
),
windowed_data AS (
  SELECT
    FIRST(sk_inverter) AS sk_inverter,
    FIRST(sk_site) AS sk_site,
    FIRST(sk_space) AS sk_space,
    WINDOW(timestamp, '15 minutes').start AS timestamp,
    CAST(AVG(input_voltage) AS DECIMAL(10,2)) AS input_voltage,
    CAST(AVG(input_current) AS DECIMAL(10,2)) AS input_current,
    CAST(AVG(input_frequency) AS DECIMAL(10,2)) AS input_frequency,
    CAST(AVG(input_power) AS DECIMAL(10,2)) AS input_power,
    CAST(AVG(ac_voltage_l1) AS DECIMAL(10,2)) AS ac_voltage_l1,
    CAST(AVG(ac_current_l1) AS DECIMAL(10,2)) AS ac_current_l1,
    CAST(AVG(ac_frequency) AS DECIMAL(10,2)) AS ac_frequency,
    CAST(AVG(ac_power) AS DECIMAL(10,2)) AS ac_power,
    LAST(charge_state) AS charge_state,
    CAST(LAST(energy_wh_hourly) AS DECIMAL(10,2)) AS energy_wh,
    CAST(AVG(temperature_cabinet) AS DECIMAL(10,2)) AS temperature_cabinet,
    CAST(AVG(temperature_ambient) AS DECIMAL(10,2)) AS temperature_ambient
  FROM with_hourly_energy
  GROUP BY
    WINDOW(timestamp, '15 minutes'),
    sk_inverter,
    sk_site,
    sk_space
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

    -- ac_energy_production: Energy (Watt-hours)
    CAST(energy_wh AS DECIMAL(10,2)) AS ac_energy_production,

    -- ac_voltage_l1: Phase-to-neutral Voltage Phase A (Volts)
    CAST(ac_voltage_l1 AS DECIMAL(10,2)) AS ac_voltage_l1,

    -- ac_frequency: Line Frequency (Hertz)
    CAST(ac_frequency AS DECIMAL(10,2)) AS ac_frequency,

    -- temperature_cabinet: Cabinet Temperature (Celsius)
    CAST(temperature_cabinet AS DECIMAL(10,2)) AS temperature_cabinet,

    -- temperature_ambient: Ambient Temperature (Celsius)
    CAST(temperature_ambient AS DECIMAL(10,2)) AS temperature_ambient,

    -- operating_state: Operating State (Enum)
    CASE
      WHEN charge_state = 'Off' THEN 'OFF'
      WHEN charge_state = 'Low power mode (search mode)' THEN 'STANDBY'
      WHEN charge_state = 'Fault' THEN 'FAULT'
      WHEN charge_state = 'Bulk' THEN 'MPPT'
      WHEN charge_state = 'Absorption' THEN 'MPPT'
      WHEN charge_state = 'Float' THEN 'MPPT'
      WHEN charge_state = 'Storage' THEN 'MPPT'
      WHEN charge_state = 'Inverting (on)' THEN 'MPPT'
      WHEN charge_state = 'External Control' THEN 'STANDBY'
      ELSE 'OFF'
    END AS operating_state

  FROM windowed_data
)

SELECT * FROM haystack_telemetry
{% if is_incremental() %}
  WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}
ORDER BY timestamp
