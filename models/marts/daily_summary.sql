{{ config(materialized='table', schema='MART',
cluster_by=['pickup_date']
)
}}

SELECT
    pickup_date,

    -- Time buckets
    COUNT_IF(time_period = 'Early morning') AS early_morning_total,
    COUNT_IF(time_period = 'Regular day') AS regular_day_total,
    COUNT_IF(time_period = 'Evening rush hour') AS evening_rush_hour_total,
    COUNT_IF(time_period = 'Night') AS night_total,
    COUNT_IF(time_period = 'Late Night') AS late_night_total,

    -- Payment
    COUNT_IF(payment_category = 'Credit card') AS credit_card_total,
    COUNT_IF(payment_category = 'Cash') AS cash_total,
    COUNT_IF(payment_category = 'Others') AS others_total,

    -- Distance
    COUNT_IF(distance_category = 'Short trip') AS short_trip_total,
    COUNT_IF(distance_category = 'Average trip') AS average_trip_total,
    COUNT_IF(distance_category = 'Long trip') AS long_trip_total,
    COUNT_IF(distance_category = 'Very long trip') AS very_long_trip_total,
    COUNT_IF(distance_category = 'Missing trip') AS missing_trip_total,

    COUNT(*) AS total_trips,

    ROUND(AVG(trip_minutes), 2) AS avg_trip_duration,
    ROUND(SUM(trip_minutes), 2) AS total_trip_duration,
    ROUND(AVG(trip_speed), 2) AS avg_trip_speed,
    ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage,
    ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
    ROUND(SUM(fare_amount), 2) AS total_fare_amount,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(SUM(total_amount), 2) AS total_amount

FROM {{ ref('int_trip_metrics') }}
GROUP BY pickup_date



