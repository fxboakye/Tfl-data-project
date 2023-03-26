{{ config(materialized = "table") }} 

WITH stations AS (
    SELECT
        DISTINCT dostation_id,
        do_station
    FROM
        {{ source("staging", "tfl") }}

    UNION DISTINCT

    SELECT
        DISTINCT pustation_id,
        pu_station
    FROM
        {{ source("staging", "tfl") }}
)

SELECT
    CAST(dostation_id as STRING) as station_id,
    CAST(do_station as STRING) as station_name
FROM
    stations