{{ config(materialized='table', schema='INTERMEDIATE') }}

select
    -- > Dimensions
    pickup_zone,
    pickup_month_name,
    time_period,
    type_of_day,
    payment_type_desc,
    payment_category,
    distance_category,
    pickup_day_name,

    -- > Metrics
    passenger_count_reporting,
    trip_minutes,
    hour_record,
    trip_speed,
    tip_percentage,
    fare_amount,
    total_amount

from {{ ref("post_clean_trips") }}
