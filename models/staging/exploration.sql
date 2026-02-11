{{ config(materialized='view') }}

WITH temp_table AS (
    SELECT 
        store_and_fwd_flag
    FROM {{ source('raw_nyc_taxi_dataset', 'YELLOW_TAXI_TRIPS') }}
)
SELECT count(*) as total_store_and_fwd_flag from temp_table where store_and_fwd_flag IS NULL
