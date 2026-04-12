{{ config(materialized='table') }}

SELECT
    air.measurement_key,
    air.indicator_key,
    dim.neighbourhood_key,
    air.start_date,
    air.end_date,
    IIF(air.season = 'annual', 1, 0) AS is_annual_measurement,
    air.indicator_value
FROM {{ ref('int_air_quality') }} air
LEFT JOIN {{ ref('dim_nyc_neighbourhoods') }} dim 
    ON air.neighbourhood = dim.neighbourhood_area
WHERE season IS NOT NULL