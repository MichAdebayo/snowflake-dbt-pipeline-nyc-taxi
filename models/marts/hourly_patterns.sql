{{ config(materialized='table',schema='MARTS',
cluster_by=['hour_record'])
}}

select hour_record, count(hour_record) as hourly_demand
from {{ ref('int_trip_metrics')}}
group by hour_record
order by hourly_demand desc