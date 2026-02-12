{{ config(materialized="table", schema="MARTS", cluster_by=["pickup_zone"]) }}

select
    pickup_zone,
    count(pickup_zone) as zonal_total_demand,
    round(avg(trip_minutes) ,2)as average_trip_duration,
    round(sum(trip_minutes), 2) as total_trip_duration,
    round(avg(trip_speed), 2) as average_trip_speed,
    round(avg(tip_percentage), 2) as average_tip_percentage,
    round(avg(fare_amount), 2) as average_fare_amount,
    round(sum(fare_amount), 2) as total_fare_amount,
    round(avg(total_amount), 2) as average_total_amount,
    round(sum(total_amount), 2) as full_total_amount
from {{ ref("int_trip_metrics") }}
group by pickup_zone
order by full_total_amount desc
