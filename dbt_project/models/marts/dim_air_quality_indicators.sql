{{ config(materialized='table') }}

SELECT 
    DISTINCT
        indicator_key,
        indicator_name,
        measure
FROM {{ ref('int_air_quality') }}