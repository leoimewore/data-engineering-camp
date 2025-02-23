{{
    config(
        materialized='view'
    )
}}

with fhv_data as (
    select
    COALESCE(REPLACE(CAST(SR_Flag AS STRING), '1', 'Shared-Rides'), 'Non-Shared-Rides') AS SR_Flag,
    CAST(pickup_datetime AS DATETIME) AS pickup_datetime,
    CAST(dropOff_datetime AS DATETIME) AS dropOff_datetime,
    * except(SR_Flag,unique_row_id, filename,pickup_datetime,dropOff_datetime)
    from 
    {{ source('staging','fhv_tripdata') }}
    where dispatching_base_num is not null
   
    
    )


select *
from 
fhv_data

