{{ config(materialized='table') }}

WITH prep_trees AS (
    -- Get all trees observed in 2015 from intermediate layer
    SELECT 
        neighbourhood,
        COUNT(DISTINCT tree_id) AS amount_of_trees,
        SUM(CASE 
            WHEN damaged_roots = 1 OR damaged_trunk = 1 OR damaged_branches = 1 OR sidewalk_damaged = 1 
            THEN 1 
            ELSE 0 
        END) AS amount_of_damaged_trees
    FROM {{ ref('int_nyc_trees') }}
    GROUP BY 
        neighbourhood
),

prep_air_quality AS (
    -- Get air quality indicators for 2015 annual measurements
    SELECT 
        dim_neigh.neighbourhood,
        ROUND(AVG(CASE 
            WHEN indicator_name = 'ozone (o3)' 
            THEN indicator_value 
        END), 2) AS ozone_o3_value,
        ROUND(AVG(CASE 
            WHEN indicator_name = 'nitrogen dioxide (no2)' 
            THEN indicator_value 
        END), 2) AS nitrogen_dioxide_no2_value,
        ROUND(AVG(CASE 
            WHEN indicator_name = 'fine particles (pm 2.5)' 
            THEN indicator_value 
        END), 2) AS fine_particles_pm25_value
    FROM {{ ref('fct_nyc_air_quality') }} air
    INNER JOIN {{ ref('dim_air_quality_indicators') }} dim_air 
        ON air.indicator_key = dim_air.indicator_key
    INNER JOIN {{ ref('dim_nyc_neighbourhoods') }} dim_neigh
        ON air.neighbourhood_key = dim_neigh.neighbourhood_key
    INNER JOIN {{ ref('dim_date') }} dim_dt
        ON dim_dt.date_key = air.start_date
    WHERE 
        dim_dt.year = 2015
        AND air.season <> 'annual' -- Ozone doesn't have annual record
        AND dim_air.indicator_name IN (
            'ozone (o3)', 
            'nitrogen dioxide (no2)', 
            'fine particles (pm 2.5)'
        )
    GROUP BY 
        dim_neigh.neighbourhood
)

SELECT 
    dim_neigh.neighbourhood_key,
    2015 AS year_key,
    trees.amount_of_trees,
    trees.amount_of_damaged_trees,
    air.ozone_o3_value,
    air.nitrogen_dioxide_no2_value,
    air.fine_particles_pm25_value,
    -- Calculated measures
    ROUND(
        CASE 
            WHEN trees.amount_of_trees > 0 
            THEN (trees.amount_of_damaged_trees * 100.0) / trees.amount_of_trees 
            ELSE 0 
        END, 2
    ) AS damage_percentage
FROM prep_air_quality air
LEFT JOIN prep_trees trees 
    ON air.neighbourhood = trees.neighbourhood
LEFT JOIN dim_nyc_neighbourhoods dim_neigh 
    ON dim_neigh.neighbourhood = air.neighbourhood