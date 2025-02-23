{{
    config(
        materialized='view'
    )
}}

with fhv_data as (
    select * ,
    from 
    {{ source('staging','fhv_tripdata') }}
    
    )