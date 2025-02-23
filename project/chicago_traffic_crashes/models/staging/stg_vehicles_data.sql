{{
    config(
        materialized='view'
    )
}}


with vehicles_data as (
    select * 
    from {{ source('staging','vehicles_data') }}
)


select * 
from 
vehicles_data

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

       limit 10

{% endif %}