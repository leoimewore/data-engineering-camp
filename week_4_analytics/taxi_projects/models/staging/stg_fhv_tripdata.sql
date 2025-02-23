{{
    config(
        materialized='view'
    )
}}

WITH fhv_tripdata AS (
    SELECT 
        COALESCE(REPLACE(CAST(SR_Flag AS STRING), '1', 'Shared-Rides'), 'Non-Shared-Rides') AS SR_Flag,
        dispatching_base_num AS dispatch_Number,
        pickup_datetime,
        dropOff_datetime AS dropoff_datetime,
        PULocationID AS pulocationid,
        DOLocationID AS dolocationid
    FROM 
        {{ source('staging', 'fhv_tripdata') }}
)

SELECT * 
FROM fhv_tripdata;


{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}