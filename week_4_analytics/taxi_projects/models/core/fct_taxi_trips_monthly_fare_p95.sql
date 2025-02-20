{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ ref('facts_trips') }}
)


select 
service_type,
pickup_year as trip_year,
pickup_month,
trip_distance,
fare_amount,
payment_type_description,

percent_rank() over (
    partition by service_type, pickup_year, pickup_month
    order by fare_amount
) as fare_percentile


from 

trips_data

where 
trip_distance > 0 and fare_amount > 0 
and pickup_year between 2019 and 2020 
and payment_type_description in ('Cash' , 'Credit Card')


