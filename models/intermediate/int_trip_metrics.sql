{{ config(materialized='table', schema='INTERMEDIATE') }}

select
   
    -- > Dimensions
    pulocationid,
    pickup_zone,
    pickup_date,
    pickup_day,
    pickup_month_name,
    time_period,
    type_of_day,
    payment_type_desc,
    payment_category,
    distance_category,
    pickup_day_name,
    hour_record,

    -- > Metrics
    passenger_count_reporting,
    trip_minutes,
    trip_speed,
    tip_percentage,
    fare_amount,
    total_amount

from {{ ref("post_clean_trips") }}
