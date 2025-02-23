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
year_quarter as quarter,

sum(total_amount) as quarterly_revenue_amount

from 

trips_data

where pickup_year between 2019 and 2020

group by 1,2,3

