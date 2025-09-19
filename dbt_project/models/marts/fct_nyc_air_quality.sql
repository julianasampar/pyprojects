{{ config(materialized='table') }}

SELECT
    measurement_key,
    indicator_key,
    neighbourhood_key,
    start_date,
    end_date,
    indicator_value
FROM {{ ref('int_air_quality') }}
LEFT JOIN {{ ref('dim_nyc_neighbourhoods') }} USING (neighbourhood)
WHERE season IS NOT NULL