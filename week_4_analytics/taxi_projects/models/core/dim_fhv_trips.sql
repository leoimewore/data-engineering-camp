{{
    config(
        materialized='table'
    )
}}

with dim_zones as (
    select 
    cast(locationid as integer) as dim_zones_locationid,

    * 
    from {{ ref('dim_zones') }}
    where borough != 'Unknown' and locationid is not null
),

fhv_data as (
    select *,
    cast(pulocationid as integer) as fhv_pulocationid,
    extract(month from pickup_datetime) as fhv_month,
    extract(year from pickup_datetime) as fhv_year
     
    from {{ ref('stg_fhv') }}
    where pulocationid is not null
)

select 
    fhv_data.*, 
    dim_zones.*,
    from fhv_data
inner join dim_zones 
    on fhv_data.fhv_pulocationid = dim_zones.dim_zones_locationid
