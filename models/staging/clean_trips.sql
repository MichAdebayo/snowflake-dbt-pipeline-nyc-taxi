{{ config(materialized='view') }}

select
    --> VendorId
    vendorid,
    case 
        when vendorid = '1' then 'Creative Mobile Technologies, LLC'
        when vendorid = '2' then 'Curb Mobility, LLC' 
        when vendorid = '6' then 'Myle Technologies Inc'
        when vendorid = '7' then 'Helix'
        else 'NONE'
    end as vendor_desc,

    --> Pickup Time
    tpep_pickup_datetime,
    cast(cast(tpep_pickup_datetime as time) as string) as pickup_time,
    cast(extract(month from tpep_pickup_datetime) as string) as pickup_month,
    cast(extract(year from tpep_pickup_datetime) as string) as pickup_year,

    --> Drop-off Times
    tpep_dropoff_datetime ,
    cast(tpep_dropoff_datetime  as time) as dropoff_time,
    cast(extract(month from tpep_dropoff_datetime ) as string) as dropoff_month,
    cast(extract(year from tpep_dropoff_datetime ) as string) as dropoff_year,
    
    --> Trip Duration
    datediff(second, tpep_pickup_datetime, tpep_dropoff_datetime) as trip_seconds,
    datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) as trip_minutes,

    --> Flag trips with zero or negative duration
    case 
        when datediff(second, tpep_pickup_datetime, tpep_dropoff_datetime) <= 0 then 1
        else 0
    end as invalid_duration_flag,
    
    --> Flag trips with zero or negative distance or fare
    case 
        when trip_distance <= 0 or fare_amount <= 0 then 1
        else 0
    end as invalid_trip_flag,

    --> Flag trips with over 24 hrs duration
    case 
        when datediff(hour, tpep_pickup_datetime, tpep_dropoff_datetime) > 24 then 1
        else 0
    end as invalid_trip_distance,

    --> Passenger Count
    case 
        when passenger_count is null then 0 else passenger_count 
    end as passenger_count_reporting,

    --> Flag missing passenger_count data
    case 
        when passenger_count is null then 1 else 0
    end as missing_passenger_count,

    --> Trip Distance
    trip_distance,

    --> Flag Extreme Trip Distance
    CASE 
        WHEN trip_distance > 100 THEN 1
        ELSE 0
    END AS extreme_distance_flag,

    --> Flag Extreme Trip Distance
    CASE 
        WHEN trip_distance > 20 THEN 1
        ELSE 0
    END AS statistical_outlier_flag,

    --> RatecodeID 
    CASE 
        WHEN ratecodeid IS NULL THEN 99
        ELSE ratecodeid
     END AS ratecodeid,
    
    --> Store and Forward Flag
    store_and_fwd_flag,
    CASE 
        WHEN store_and_fwd_flag IS NULL THEN 1
        ELSE 0
    END AS missing_store_and_fwd_flag,

    CASE 
        WHEN store_and_fwd_flag = '1' then 'Standard rate'
        WHEN store_and_fwd_flag = '2' then 'JFK'
        WHEN store_and_fwd_flag = '3' then 'Newark '
        WHEN store_and_fwd_flag = '4' then 'Nassau or Westchester'
        WHEN store_and_fwd_flag = '5' then 'Negotiated fare'
        WHEN store_and_fwd_flag = '6' then 'Group ride'
        ELSE 'Null/unknown'
    END AS store_and_fwd_flag_desc,

    
from {{ source('raw_nyc_taxi_dataset', 'YELLOW_TAXI_TRIPS') }}

