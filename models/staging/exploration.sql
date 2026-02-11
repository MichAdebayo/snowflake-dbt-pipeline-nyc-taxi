{{ config(materialized='view') }}

WITH temp_table AS (
    SELECT 
        cbd_congestion_fee 
    FROM {{ source('raw_nyc_taxi_dataset', 'YELLOW_TAXI_TRIPS') }}
)
SELECT count(*) as tot from temp_table where cbd_congestion_fee < 0
