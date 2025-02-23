{{
    config(
        materialized='view'
    )
}}


WITH persons_data AS (
    SELECT  
        *,
        CASE 
            WHEN SAFE_CAST(safety_equipment AS STRING) IS NULL OR SAFE_CAST(safety_equipment AS STRING) = '' THEN 'USAGE UNKNOWN'
            ELSE SAFE_CAST(safety_equipment AS STRING)
        END AS safety_equipment_updated
    FROM {{ source('staging','persons_data') }}
    WHERE age IS NOT NULL 
    AND SAFE_CAST(age AS INT64) IS NOT NULL
)




select * 
from 
persons_data




