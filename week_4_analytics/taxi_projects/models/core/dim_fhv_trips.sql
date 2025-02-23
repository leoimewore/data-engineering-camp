{{
    config(
        materialized='table'
    )
}}


with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

fhv_tripdata as (
    select * from {{ref('stg_fhv_tripdata')}}
)

dim_fhv_joined as (
    select * from dim_zones
    union all
    select * from fhv_tripdata
)

-- select
-- {{ dbt.date_trunc("month","fhv_tripdata.pickup_datetime") }} as pickup_month,
