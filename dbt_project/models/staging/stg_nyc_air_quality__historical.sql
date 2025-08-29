{{ config(materialized='table') }}

SELECT 
    "Unique ID" AS unique_id,
    "Indicator ID" AS indicator_id,
    LOWER("Name") AS indicator_name,
    LOWER("Measure") AS measure,
    LOWER("Measure Info") AS measure_info,
    LOWER("Geo Type Name") AS geo_type_name,
    "Geo Join ID" AS geo_join_id,
    LOWER("Geo Place Name") AS geo_place_name,
    LOWER("Time Period") AS time_period,
    CAST(SUBSTR("Start_Date", -4) 
        || SUBSTR("Start_Date", 0, 3) 
        || SUBSTR("Start_Date", 4, 2)
        AS INT) AS start_date,
    "Data Value" AS value,
    "Message" AS message,
    CURRENT_TIMESTAMP AS updated_at
FROM nyc_air_quality__historical