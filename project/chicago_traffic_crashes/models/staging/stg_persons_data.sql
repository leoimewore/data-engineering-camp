{{
    config(
        materialized='view'
    )
}}


WITH persons_data AS (
    SELECT  
        *,
        {{fill_safety_equipment('safety_equipment')}} as safety_equipment_updated,
    FROM {{ source('staging','person_data') }}
    WHERE  age != 0
    
    
)




select * 
from 
persons_data

-- dbt build --select <model.sql> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}




