{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('acme_catalog', 'acme_dev') }}.gold.ecosphere_buildings"
  )
}}

SELECT
  MD5(id) AS sk_building,
  *
FROM {{ ref('ecosphere_buildings') }}
