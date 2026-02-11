{{ config(materialized='view') }}

with temp_table as (
    select 
        tpep_pickup_datetime,
        cast(tpep_pickup_datetime as time) as record_time,
        cast(extract(month from tpep_pickup_datetime) as string) as record_month,
        cast(extract(year from tpep_pickup_datetime) as string) as record_year
    from {{ source('raw_nyc_taxi_dataset', 'YELLOW_TAXI_TRIPS') }}
    )
select * from temp_table 
WHERE TO_VARCHAR(record_year) IN ('', ' ', 'NONE', 'null')

