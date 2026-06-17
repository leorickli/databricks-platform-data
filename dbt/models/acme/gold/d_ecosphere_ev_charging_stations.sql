{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('acme_catalog', 'acme_dev') }}.gold.ecosphere_ev_charging_stations"
  )
}}

SELECT
  MD5(id) AS sk_evchargingstation,
  *
FROM {{ ref('ecosphere_ev_charging_stations') }}
