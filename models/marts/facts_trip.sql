{{ config(materialized='table', schema='MARTS') }}

select *
from {{ ref("int_trip_metrics") }}
