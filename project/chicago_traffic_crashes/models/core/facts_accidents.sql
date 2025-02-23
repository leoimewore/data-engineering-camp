{{
    config(
        materialized='table'
    )
}}

with persons_data as (
    select * from 
    {{ ref('stg_persons_data') }}
    WHERE age IS NOT NULL 
    AND SAFE_CAST(age AS INT64) IS NOT NULL
)


select
*
from persons_data