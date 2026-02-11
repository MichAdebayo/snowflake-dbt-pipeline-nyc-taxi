{{ config(materialized='view') }}

WITH temp_table AS (
    SELECT 
        DOLocationID 
    FROM {{ source('raw_nyc_taxi_dataset', 'YELLOW_TAXI_TRIPS') }}
)
SELECT count(*) as c_DOLocationID  from temp_table where DOLocationID  is null
