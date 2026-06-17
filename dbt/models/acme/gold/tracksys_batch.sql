{{ config(cluster_by='auto') }}

SELECT
    sha2(cast(loggerImei AS STRING), 256)                  AS skAsset,
    window(timestamp, '15 minutes').start                  AS timestamp,
    to_date(window(timestamp, '15 minutes').start)         AS eventDate,
    loggerImei,
    ROUND(AVG(SoC), 2)                                     AS SoC,
    ROUND(AVG(voltage), 2)                                 AS voltage,
    ROUND(AVG(current), 2)                                 AS current,
    ROUND(AVG(activePower), 2)                             AS activePower,
    current_timestamp()                                    AS goldProcessingTimestamp
FROM
    {{ source('acme', 'tracksys_batch') }}

{% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}

GROUP BY
    loggerImei,
    window(timestamp, '15 minutes')
