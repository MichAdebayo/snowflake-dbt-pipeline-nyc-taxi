{{ config(materialized="table") }}

select
    -- > VendorId
    vendorid,
    case
        when vendorid = '1'
        then 'Creative Mobile Technologies, LLC'
        when vendorid = '2'
        then 'Curb Mobility, LLC'
        when vendorid = '6'
        then 'Myle Technologies Inc'
        when vendorid = '7'
        then 'Helix'
        else 'NONE'
    end as vendor_desc,

    -- > Pickup Time
    tpep_pickup_datetime,
    cast(cast(tpep_pickup_datetime as time) as string) as pickup_time,
    cast(extract(day from tpep_pickup_datetime) as string) as pickup_day,
    cast(extract(month from tpep_pickup_datetime) as string) as pickup_month,
    cast(extract(year from tpep_pickup_datetime) as string) as pickup_year,

    -- > Drop-off Times
    tpep_dropoff_datetime,
    cast(tpep_dropoff_datetime as time) as dropoff_time,
    cast(extract(day from tpep_dropoff_datetime) as string) as dropoff_day,
    cast(extract(month from tpep_dropoff_datetime) as string) as dropoff_month,
    cast(extract(year from tpep_dropoff_datetime) as string) as dropoff_year,

    -- > Trip Duration
    datediff(second, tpep_pickup_datetime, tpep_dropoff_datetime) as trip_seconds,
    datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) as trip_minutes,

    -- > Flag trips with zero or negative duration
    case
        when datediff(second, tpep_pickup_datetime, tpep_dropoff_datetime) <= 0
        then 1
        else 0
    end as invalid_duration_flag,

    -- > Flag trips with zero or negative distance or fare
    case
        when trip_distance <= 0 or fare_amount <= 0 then 1 else 0
    end as invalid_trip_flag,

    -- > Flag trips with over 24 hrs duration
    case
        when datediff(hour, tpep_pickup_datetime, tpep_dropoff_datetime) > 24
        then 1
        else 0
    end as invalid_trip_distance,

    -- > Passenger Count
    case
        when passenger_count is null then 0 else passenger_count
    end as passenger_count_reporting,

    -- > Flag missing passenger_count data
    case when passenger_count is null then 1 else 0 end as missing_passenger_count,

    -- > Trip Distance
    trip_distance,

    -- > Flag Extreme Trip Distance (expected to be reported)
    case when trip_distance not between 0.1 and 100 then 1 else 0 end as extreme_distance_flag,

    -- > Flag Outlier Trip Distance
    case when trip_distance > 20 then 1 else 0 end as statistical_outlier_flag,

    -- > RatecodeID 
    case when ratecodeid is null then 99 else ratecodeid end as ratecodeid,

    case
        when ratecodeid  = '1'
        then 'Standard rate'
        when ratecodeid  = '2'
        then 'JFK'
        when ratecodeid  = '3'
        then 'Newark '
        when ratecodeid  = '4'
        then 'Nassau or Westchester'
        when ratecodeid  = '5'
        then 'Negotiated fare'
        when ratecodeid  = '6'
        then 'Group ride'
        else 'Null/unknown'
    end as ratecodeid_desc,

    -- > Store and Forward Flag
    store_and_fwd_flag,
    case
        when store_and_fwd_flag is null then 1 else 0
    end as missing_store_and_fwd_flag,

    -- > TLC Taxi Zones
    pulocationid,
    dolocationid,

    -- > Payment Type
    payment_type,
    case
        when payment_type = '0'
        then 'Flex Fare trip'
        when payment_type = '1'
        then 'Credit card'
        when payment_type = '2'
        then 'Cash'
        when payment_type = '3'
        then 'No charge'
        when payment_type = '4'
        then 'Dispute'
        when payment_type = '5'
        then 'Unknown'
        else 'Voided trip'
    end as payment_type_desc,

    -- > Fare Amount 
    fare_amount,
    case when fare_amount < 0 then 1 else 0 end as fare_amount_flag,

    -- > Extra 
    extra,
    case when extra < 0 then 1 else 0 end as extra_flag,

    -- > Mta Tax 
    mta_tax,
    case when mta_tax < 0 then 1 else 0 end as mta_tax_flag,

    -- > Tip Amount
    tip_amount,
    case when tip_amount < 0 then 1 else 0 end as tip_amount_flag,

    -- > Tolls Amount
    tolls_amount,
    case when tolls_amount < 0 then 1 else 0 end as tolls_amount_flag,

    -- > Improvement Surcharge
    improvement_surcharge,
    case when improvement_surcharge < 0 then 1 else 0 end as improvement_surcharge_flag,

    -- > Total Amount 
    total_amount,
    case when total_amount < 0 then 1 else 0 end as total_amount_flag,

    -- > Congestion Surcharge 
    congestion_surcharge,
    case when congestion_surcharge < 0 then 1 else 0 end as invalid_congestion_surcharge_flag,
    case when congestion_surcharge is null then 1 else 0 end as missing_congestion_surcharge_flag,

    -- > Airport Fee
    airport_fee,
    case when airport_fee < 0 then 1 else 0 end as invalid_airport_fee_flag,
    case when airport_fee  is null then 1 else 0 end as missing_airport_fee_flag,

    -- > CBD Congestion Fee 
    cbd_congestion_fee ,
    case when cbd_congestion_fee < 0 then 1 else 0 end as invalid_cbd_congestion_fee_flag
from {{ source("raw_nyc_taxi_dataset", "YELLOW_TAXI_TRIPS") }}
