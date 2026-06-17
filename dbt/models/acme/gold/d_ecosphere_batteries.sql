{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('acme_catalog', 'acme_dev') }}.gold.ecosphere_batteries"
  )
}}

SELECT
  MD5(CONCAT(building_uuid, '_battery')) AS sk_battery,
  *
FROM {{ ref('ecosphere_batteries') }}
