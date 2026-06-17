WITH silver_data AS (
  SELECT
    -- Identifiers (snake_case, ESDL v2 ontology)
    dap_id,
    ean,
    key,

    -- Time dimensions
    timestamp,
    measurement_date,

    -- Energy measurements (kWh and kVARh)
    energy_import,
    energy_export,
    reactive_energy_import,
    reactive_energy_export,

    -- Contextual attributes (timestamp-level)
    tariff_rate,
    is_peak_demand

  FROM {{ source('acme_silver', 'wattflow_dap_batch') }}

  {% if is_incremental() %}
    WHERE timestamp > COALESCE((SELECT MAX(timestamp) - INTERVAL 1 HOUR FROM {{ this }}), '1970-01-01')
  {% endif %}
),

-- Calculate surrogate key and derived metrics
enriched AS (
  SELECT
    *,
    -- Generate surrogate key using same logic as dimension table (ean + key)
    MD5(CONCAT(ean, key)) AS sk_econnection,

    -- Total active energy (import + export) in kWh
    COALESCE(energy_import, 0) + COALESCE(energy_export, 0) AS energy_throughput,

    -- Total reactive energy (import + export) in kVARh
    COALESCE(reactive_energy_import, 0) + COALESCE(reactive_energy_export, 0) AS reactive_energy_throughput,

    -- Net active energy (import - export) in kWh
    COALESCE(energy_import, 0) - COALESCE(energy_export, 0) AS energy_net,

    -- Net reactive energy (import - export) in kVARh
    COALESCE(reactive_energy_import, 0) - COALESCE(reactive_energy_export, 0) AS reactive_energy_net

  FROM silver_data
)

SELECT
  -- Natural keys (ean + key form the unique meter identifier)
  dap_id,
  ean,
  key,
  sk_econnection,

  -- Time dimensions
  timestamp,
  measurement_date,

  -- Contextual attributes
  tariff_rate,
  is_peak_demand,

  -- Base energy measurements (from silver)
  energy_import,
  energy_export,
  reactive_energy_import,
  reactive_energy_export,

  -- Derived metrics (calculated in gold)
  energy_throughput,
  reactive_energy_throughput,
  energy_net,
  reactive_energy_net,

  -- Processing metadata
  CURRENT_TIMESTAMP() AS gold_processing_timestamp

FROM enriched

{% if is_incremental() %}
  WHERE timestamp > COALESCE((SELECT MAX(timestamp) FROM {{ this }}), '1970-01-01')
{% endif %}

ORDER BY timestamp, dap_id
