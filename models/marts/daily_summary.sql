{{ config(materialized="table", schema="MARTS", cluster_by=["pickup_date"]) }}

select
    pickup_date,

    -- Time buckets
    count_if(time_period = 'Early morning') as early_morning_total,
    count_if(time_period = 'Regular day') as regular_day_total,
    count_if(time_period = 'Evening rush hour') as evening_rush_hour_total,
    count_if(time_period = 'Night') as night_total,
    count_if(time_period = 'Late Night') as late_night_total,

    -- Payment
    count_if(payment_category = 'Credit card') as credit_card_total,
    count_if(payment_category = 'Cash') as cash_total,
    count_if(payment_category = 'Others') as others_total,

    -- Distance
    count_if(distance_category = 'Short trip') as short_trip_total,
    count_if(distance_category = 'Average trip') as average_trip_total,
    count_if(distance_category = 'Long trip') as long_trip_total,
    count_if(distance_category = 'Very long trip') as very_long_trip_total,
    count_if(distance_category = 'Missing trip') as missing_trip_total,

    count(*) as total_trips,

    round(avg(trip_minutes), 2) as avg_trip_duration,
    round(sum(trip_minutes), 2) as total_trip_duration,
    round(avg(trip_speed), 2) as avg_trip_speed,
    round(avg(tip_percentage), 2) as avg_tip_percentage,
    round(avg(fare_amount), 2) as avg_fare_amount,
    round(sum(fare_amount), 2) as total_fare_amount,
    round(avg(total_amount), 2) as avg_total_amount,
    round(sum(total_amount), 2) as total_amount

from {{ ref("int_trip_metrics") }}
group by pickup_date
order by pickup_date
