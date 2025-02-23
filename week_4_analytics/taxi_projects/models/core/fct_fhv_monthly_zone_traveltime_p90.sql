{{
    config(
        materialized='table'
    )
}}

with dim_fhv as (
    select *, 

    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, minute) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)

select * from dim_fhv