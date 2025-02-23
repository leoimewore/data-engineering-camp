{{
    config(
        materialized='view'
    )
}}


with traffic_crashes_data as (
    select * 
    from {{ source('staging','traffic_data') }}
)


select * 
from 
traffic_crashes_data

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

       limit 10

{% endif %}