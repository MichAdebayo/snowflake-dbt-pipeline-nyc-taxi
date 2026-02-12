{{ config(materialized='view') }}

WITH temp_table AS (
    SELECT 
      tpep_pickup_datetime, tpep_dropoff_datetime
    FROM {{ source('raw_nyc_taxi_dataset', 'YELLOW_TAXI_TRIPS') }}
)
SELECT extract(dow from tpep_pickup_datetime) as pickup_dow, to_char(tpep_pickup_datetime, 'Day') as pickup_day_name from temp_table 
