{{
    config(
        materialized='table'
    )
}}

with persons_data as (
    select 
    person_id,
    crash_record_id,
    vehicle_id,
    crash_date,
    sex,
    safety_equipment_updated,
    age,
     from 
    {{ ref('stg_persons_data') }}

)


select
*
from persons_data