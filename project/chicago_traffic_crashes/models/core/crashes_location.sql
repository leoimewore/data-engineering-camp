{{
    config(
        materialized='table'
    )
}}

with crash_info as (
    select *
    from {{ ref('stg_traffic_data') }}
)

select
longitude,
latitude,
injuries_total,
injuries_fatal
from 
crash_info
