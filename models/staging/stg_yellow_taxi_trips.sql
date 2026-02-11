{{ config(materialized='view') }}

select
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    total_amount
from {{ source('raw_nyc_taxi', 'YELLOW_TAXI_TRIPS') }}
