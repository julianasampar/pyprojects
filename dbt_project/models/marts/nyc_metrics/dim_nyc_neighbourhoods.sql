{{ config(materialized='table') }}

WITH neighbourhoods AS (
    SELECT
        DISTINCT
            {{ standardize_nulls('neighbourhood') }} AS neighbourhood_area
    FROM {{ ref('int_nyc_trees') }}

    UNION

    SELECT
        DISTINCT
            {{ standardize_nulls('neighbourhood') }} AS neighbourhood_area
    FROM {{ ref('int_air_quality') }}
)

SELECT
    ROW_NUMBER() OVER(ORDER BY neighbourhood_area) AS neighbourhood_key,
    CASE
        WHEN neighbourhood_area LIKE '%-%'
            THEN SUBSTR(neighbourhood_area, 0, INSTR(neighbourhood_area, '-'))
        WHEN neighbourhood_area LIKE '%(%'
            THEN SUBSTR(neighbourhood_area, 0, INSTR(neighbourhood_area, '('))
        WHEN neighbourhood_area LIKE '% and %'
            THEN SUBSTR(neighbourhood_area, 0, INSTR(neighbourhood_area, 'and'))
        WHEN neighbourhood_area LIKE '% east'
            THEN SUBSTR(neighbourhood_area, 0, INSTR(neighbourhood_area, 'east'))
        WHEN neighbourhood_area LIKE '% west'
            THEN SUBSTR(neighbourhood_area, 0, INSTR(neighbourhood_area, 'west'))
        WHEN neighbourhood_area LIKE '% south'
            THEN SUBSTR(neighbourhood_area, 0, INSTR(neighbourhood_area, 'south'))
        WHEN neighbourhood_area LIKE '% north'
            THEN SUBSTR(neighbourhood_area, 0, INSTR(neighbourhood_area, 'north'))
        ELSE neighbourhood_area
    END AS neighbourhood,
    neighbourhood_area
FROM neighbourhoods
WHERE neighbourhood_area IS NOT NULL