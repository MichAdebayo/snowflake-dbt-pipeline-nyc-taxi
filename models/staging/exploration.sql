{{ config(materialized='view') }}

WITH temp_table AS (
    SELECT 
        congestion_surcharge 
    FROM {{ source('raw_nyc_taxi_dataset', 'YELLOW_TAXI_TRIPS') }}
)
SELECT count(*) as tot from temp_table where congestion_surcharge < 0
