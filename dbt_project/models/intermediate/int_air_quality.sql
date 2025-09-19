{{ config(materialized='table') }}

WITH prep_air_quality AS (
    SELECT 
        unique_id AS measurement_key,
        start_date,
        CASE 
            WHEN time_period NOT LIKE '%annual%'
            THEN LEAD(start_date, 1) 
                OVER(PARTITION BY indicator_id, geo_place_name ORDER BY start_date ASC)
            WHEN time_period LIKE '%annual%'
                THEN start_date + 10000
        END AS end_date,
        indicator_id AS indicator_key,
        indicator_name,
        measure,
        measure_info,
        TRIM(
                SUBSTR(geo_place_name, 0, INSTR(geo_place_name, '(')) 
            ) AS neighbourhood,
        SUBSTR(time_period, 0, INSTR(time_period, ' ')) AS season,
        ROUND(value, 3) AS indicator_value
    FROM {{ ref('stg_nyc_air_quality__historical') }}
)
SELECT 
    measurement_key,
    start_date,
    end_date,
    indicator_key,
    {{ standardize_nulls('indicator_name') }} AS indicator_name,
    {{ standardize_nulls('measure') }} AS measure,
    {{ standardize_nulls('measure_info') }} AS measure_info,
    {{ standardize_nulls('neighbourhood') }} AS neighbourhood,
    {{ standardize_nulls('season') }} AS season,
    indicator_value
FROM prep_air_quality