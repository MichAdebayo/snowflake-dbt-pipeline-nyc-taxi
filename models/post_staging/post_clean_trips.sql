{{ config(
    materialized='table',
    schema='STAGING'
) }}


with
    post_clean as (
        select
            vendor_desc,
            pickup_time,
            pickup_month,
            case
                when pickup_month = '1'
                then 'January'
                when pickup_month = '2'
                then 'February'
                when pickup_month = '3'
                then 'March'
                when pickup_month = '4'
                then 'April'
                when pickup_month = '5'
                then 'May'
                when pickup_month = '6'
                then 'June'
                when pickup_month = '7'
                then 'July'
                when pickup_month = '8'
                then 'August'
                when pickup_month = '9'
                then 'September'
                when pickup_month = '10'
                then 'October'
                when pickup_month = '11'
                then 'November'
                else 'December'
            end as pickup_month_name,
            case
                when extract(hour from to_time(pickup_time)) between 6 and 9
                then 'Early morning'
                when extract(hour from to_time(pickup_time)) between 10 and 15
                then 'Regular day'
                when extract(hour from to_time(pickup_time)) between 16 and 19
                then 'Evening rush hour'
                when extract(hour from to_time(pickup_time)) between 20 and 23
                then 'Night'
                else 'Late Night'
            end as time_period,
            extract(dow from tpep_pickup_datetime) as pickup_dow,
            initcap(
                case
                    extract(dow from tpep_pickup_datetime)
                    when 0
                    then 'Sunday'
                    when 1
                    then 'Monday'
                    when 2
                    then 'Tuesday'
                    when 3
                    then 'Wednesday'
                    when 4
                    then 'Thursday'
                    when 5
                    then 'Friday'
                    when 6
                    then 'Saturday'
                end
            ) as pickup_day_name,
            pickup_year,
            dropoff_day,
            dropoff_month,
            dropoff_year,
            trip_seconds,
            trip_minutes,
            round(trip_minutes / 60, 2) as trip_hour,
            round(trip_distance / Coalesce(NULLIF(trip_minutes / 60, 0), 0), 2) as trip_speed,
            passenger_count_reporting,
            trip_distance,
            case
                when trip_distance between 0 and 1
                then 'short trip'
                when trip_distance between 2 and 5
                then 'average trip'
                when trip_distance between 6 and 10
                then 'long trip'
                when trip_distance > 10
                then 'very long trip'
                else 'misssing trip'
            end as distance_category,
            ratecodeid_desc,
            store_and_fwd_flag,
            pulocationid,
            dolocationid,
            payment_type_desc,
            case
                when payment_type_desc = 'Credit card'
                then payment_type_desc
                when payment_type_desc = 'Cash'
                then payment_type_desc
                else 'Others'
            end as payment_category,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            coalesce(
                round((tip_amount / nullif(total_amount, 0)) * 100, 2), 0
            ) as tip_percentage,
            congestion_surcharge,
            airport_fee,
            cbd_congestion_fee
        from {{ ref("clean_trips") }}
        where
            invalid_duration_flag = 0
            and invalid_trip_flag = 0
            and invalid_trip_distance = 0
            and extreme_distance_flag = 0
            and missing_store_and_fwd_flag = 0
            and fare_amount_flag = 0
            and extra_flag = 0
            and mta_tax_flag = 0
            and tip_amount_flag = 0
            and tolls_amount_flag = 0
            and improvement_surcharge_flag = 0
            and total_amount_flag = 0
            and invalid_congestion_surcharge_flag = 0
            and missing_congestion_surcharge_flag = 0
            and invalid_airport_fee_flag = 0
            and missing_airport_fee_flag = 0
            and invalid_cbd_congestion_fee_flag = 0
            and trip_minutes != 0
    )

select
    *,
    case
        when pickup_day_name in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
        then 'Work week'
        else 'Weekend'
    end as type_of_day
from post_clean pc
left join {{ ref('taxi_zone_lookup')}} pu on 
 pc.pulocationid = pu.locationid
left join {{ ref('taxi_zone_lookup')}} do on 
 pc.pulocationid = do.locationid


