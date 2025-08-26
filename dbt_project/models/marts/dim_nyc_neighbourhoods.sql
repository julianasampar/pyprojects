{{ config(materialized='table') }}

/*
SELECT 1 AS neighbourhood_key, 'battery park city-lower manhattan' AS neighbourhood
UNION ALL
SELECT 2, 'central harlem north-polo grounds'
UNION ALL
SELECT 3, 'central harlem south'
UNION ALL
SELECT 4, 'chinatown'
UNION ALL
SELECT 5, 'clinton'
UNION ALL
SELECT 6, 'east harlem north'
UNION ALL
SELECT 7, 'east harlem south'
UNION ALL
SELECT 8, 'east village'
UNION ALL
SELECT 9, 'gramercy'
UNION ALL
SELECT 10, 'hamilton heights'
UNION ALL
SELECT 11, 'hudson yards-chelsea-flatiron-union square'
UNION ALL
SELECT 12, 'lenox hill-roosevelt island'
UNION ALL
SELECT 13, 'lincoln square'
UNION ALL
SELECT 14, 'lower east side'
UNION ALL
SELECT 15, 'manhattanville'
UNION ALL
SELECT 16, 'marble hill-inwood'
UNION ALL
SELECT 17, 'midtown-midtown south'
UNION ALL
SELECT 18, 'morningside heights'
UNION ALL
SELECT 19, 'murray hill-kips bay'
UNION ALL
SELECT 20, 'soho-tribeca-civic center-little italy'
UNION ALL
SELECT 21, 'stuyvesant town-cooper village'
UNION ALL
SELECT 22, 'turtle bay-east midtown'
UNION ALL
SELECT 23, 'upper east side-carnegie hill'
UNION ALL
SELECT 24, 'upper west side'
UNION ALL
SELECT 25, 'washington heights north'
UNION ALL
SELECT 26, 'washington heights south'
UNION ALL
SELECT 27, 'west village'
UNION ALL
SELECT 28, 'yorkville';*/

WITH neighbourhoods AS (
    SELECT
        DISTINCT 
            TRIM(neighbourhood) AS neighbourhood
    FROM {{ ref('int_manhattan_trees') }}

    UNION

    SELECT
        DISTINCT 
            neighbourhood
    FROM {{ ref('int_air_quality') }}
)

SELECT
    ROW_NUMBER() OVER(ORDER BY neighbourhood) AS neighbourhood_key,
    neighbourhood
FROM neighbourhoods