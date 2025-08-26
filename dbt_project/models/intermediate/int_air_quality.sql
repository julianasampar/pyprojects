{{ config(materialized='table') }}

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