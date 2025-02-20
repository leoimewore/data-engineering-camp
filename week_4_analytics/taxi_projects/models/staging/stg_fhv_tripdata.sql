{{
    config(
        materialized='view'
    )
}}

with fhv_tripdata as 
(
    select 
    coalesce(replace(cast(SR_Flag as string),'1' , 'Shared-Rides'), 'Non-Shared-Rides') as SR_Flag,
    dispatching_base_num as dispacth_Number,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    PULocationID,
    DOLocationID
   
    from 
    {{ source('staging','fhv_tripdata') }}
    
)

select * from fhv_tripdata