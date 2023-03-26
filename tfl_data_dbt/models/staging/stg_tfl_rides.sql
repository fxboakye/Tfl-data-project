{{ config(materialized = "view") }} 

WITH tfldata AS (
    SELECT
        rental_id,
        bike_id,
        pickup_datetime,
        pustation_id,
        pu_station,
        dropoff_datetime,
        dostation_id,
        do_station
    FROM
        {{ source("staging", "tfl")}}
    WHERE
        rental_id is not null
)

--Parses datetime depending on their formats
SELECT
    CAST(rental_id AS INT64) AS rental_id,
    CAST(bike_id AS INT64) AS bike_id,
    {{parse_datetime('pickup_datetime')}} AS pickup_datetime,
    CAST(pustation_id AS STRING) AS pustation_id,
    {{parse_datetime('dropoff_datetime')}} AS dropoff_datetime,
    CAST(dostation_id AS STRING) AS dostation_id
from
    tfldata
