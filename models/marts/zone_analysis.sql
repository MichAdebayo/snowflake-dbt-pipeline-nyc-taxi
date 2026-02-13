{{ config(materialized="table", schema="MARTS", cluster_by=["pickup_zone"]) }}

select
    pickup_zone,

    sum(
        case when distance_category = 'Short trip' then 1 else 0 end
    ) as short_trip_total,
    sum(
        case when distance_category = 'Average trip' then 1 else 0 end
    ) as average_trip_total,
    sum(case when distance_category = 'Long trip' then 1 else 0 end) as long_trip_total,
    sum(
        case when distance_category = 'Very long trip' then 1 else 0 end
    ) as very_long_trip_total,

    sum(
        case when payment_category = 'Credit card' then 1 else 0 end
    ) as credit_card_total,
    sum(case when payment_category = 'Cash' then 1 else 0 end) as cash_total,
    sum(case when payment_category = 'Others' then 1 else 0 end) as others_total,

    count(pickup_zone) as trip_total,
    round(avg(trip_minutes), 2) as average_trip_duration,
    round(sum(trip_minutes), 2) as total_trip_duration,
    round(avg(trip_speed), 2) as average_trip_speed,
    round(avg(tip_percentage), 2) as average_tip_percentage,
    round(avg(fare_amount), 2) as average_fare_amount,
    round(sum(fare_amount), 2) as total_fare_amount,
    round(avg(total_amount), 2) as average_total_amount,
    round(sum(total_amount), 2) as full_total_amount
from {{ ref("int_trip_metrics") }}
group by pickup_zone
order by trip_total desc
