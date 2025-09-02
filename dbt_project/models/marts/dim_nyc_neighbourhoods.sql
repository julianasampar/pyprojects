{{ config(materialized='table') }}

WITH neighbourhoods AS (
    SELECT
        DISTINCT
            {{ standardize_nulls('neighbourhood') }} AS neighbourhood
    FROM {{ ref('int_manhattan_trees') }}

    UNION

    SELECT
        DISTINCT
            {{ standardize_nulls('neighbourhood') }} AS neighbourhood
    FROM {{ ref('int_air_quality') }}
)

SELECT
    ROW_NUMBER() OVER(ORDER BY neighbourhood) AS neighbourhood_key,
    neighbourhood
FROM neighbourhoods
WHERE neighbourhood IS NOT NULL