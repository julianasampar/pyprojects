
    
    create view main."fct_nyc_trees_with_ozone" as
    


WITH ozone_measurements AS (
    -- Get annual ozone measurements from mart layer
    SELECT 
        aq.neighbourhood_key,
        aq.start_date,
        aq.end_date,
        aq.indicator_value AS ozone_value,
        ind.measure AS ozone_measure_unit,
        n.neighbourhood
    FROM main."fct_nyc_air_quality" aq
    INNER JOIN main."dim_air_quality_indicators" ind 
        ON aq.indicator_key = ind.indicator_key
    INNER JOIN main."dim_nyc_neighbourhoods" n
        ON aq.neighbourhood_key = n.neighbourhood_key
    WHERE (LOWER(ind.indicator_name) LIKE '%ozone%' 
           OR LOWER(ind.indicator_name) LIKE '%o3%'
           OR LOWER(ind.measure) LIKE '%ozone%'
           OR LOWER(ind.measure) LIKE '%o3%')
      AND aq.season = 'annual'  -- Only annual measurements as requested
      AND aq.indicator_value IS NOT NULL
)

-- Join trees with ozone data using neighbourhood and date range matching
-- Only return trees that have matching ozone data (INNER JOIN)
SELECT 
    t.tree_id,
    t.observation_date,
    t.latitude,
    t.longitude,
    t.neighbourhood,
    t.borough,
    t.species_common_name,
    t.species_latin_name,
    t.tree_status,
    t.is_tree_alive,
    t.tree_on_curb,
    t.tree_diameter,
    t.damaged_roots,
    t.damaged_trunk,
    t.damaged_branches,
    t.sidewalk_damaged,
    o.ozone_value,
    o.ozone_measure_unit,
    o.start_date AS air_quality_start_date,
    o.end_date AS air_quality_end_date
FROM main."int_nyc_trees" t
INNER JOIN ozone_measurements o 
    ON LOWER(TRIM(t.neighbourhood)) = LOWER(TRIM(o.neighbourhood))
    AND t.observation_date >= o.start_date 
    AND t.observation_date <= COALESCE(o.end_date, o.start_date + 10000)  -- Handle null end_dates by assuming 1-year period;