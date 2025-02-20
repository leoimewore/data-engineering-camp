{% macro get_quarter(lpep_pickup_datetime) %}
    CASE 
        WHEN EXTRACT(MONTH FROM {{ lpep_pickup_datetime }}) IN (1, 2, 3) THEN 1
        WHEN EXTRACT(MONTH FROM {{ lpep_pickup_datetime }}) IN (4, 5, 6) THEN 2
        WHEN EXTRACT(MONTH FROM {{ lpep_pickup_datetime }}) IN (7, 8, 9) THEN 3
        WHEN EXTRACT(MONTH FROM {{ lpep_pickup_datetime }}) IN (10, 11, 12) THEN 4
        ELSE NULL
    END
{% endmacro %}