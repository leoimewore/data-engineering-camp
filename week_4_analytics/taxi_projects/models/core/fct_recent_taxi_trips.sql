{{
    config(
        materialized='table'
    )
}}

select * 
from {{ ref('facts_trips') }}
where DATE(pickup_datetime) >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY 
