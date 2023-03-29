{{ config(materialized = "table") }} 

WITH cleaned_tfldata AS (
    SELECT
        *
    FROM
        {{ ref('tfl_fact') }}
),
stations AS (
    SELECT
        *
    FROM
        {{ ref('stg_tfl_station') }}
)
SELECT
    pickup_datetime,
    station_name,
    COUNT(rental_id) as total_rides
FROM
    cleaned_tfldata
    JOIN stations ON stations.station_id = cleaned_tfldata.pustation_id
GROUP BY 1,2