{{ config(materialized='table') }}

SELECT
    tree_id,
    observation_date,
    ROUND(latitude, 3)  AS latitude,
    ROUND(longitude, 3) AS longitude,
    city,
    borough,
    TRIM(neighbourhood) AS neighbourhood,
    tree_diameter,
    stump_diameter,
    CASE
        WHEN tree_status = 'alive'
        THEN 1 ELSE 0 
    END AS is_tree_alive,
    CASE
        WHEN tree_curb_location = 'oncurb'
        THEN 1 ELSE 0 
    END tree_on_curb,
    species_common_name,
    species_latin_name,
    CASE 
        WHEN sidewalk_damaged = 'damage'
        THEN 1 ELSE 0
    END AS sidewalk_damaged,
    CASE 
        WHEN damaged_roots_by_paving_stones = 'yes'
        OR damaged_trunk_by_lights = 'yes'
        OR damaged_roots_by_other = 'yes'
        THEN 1 ELSE 0
    END AS damaged_roots,
    CASE 
        WHEN damaged_trunk_by_lights = 'yes'
        OR damaged_trunk_by_rope_or_wires = 'yes'
        OR damaged_trunk_by_other = 'yes'
        THEN 1 ELSE 0
    END AS damaged_trunk,
    CASE 
        WHEN damaged_branch_by_lights_or_wire = 'yes'
        OR damaged_branch_by_shoes = 'yes'
        OR damaged_branch_by_other = 'yes'
        THEN 1 ELSE 0
    END AS damaged_branches,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg__nyc_street_tree_census__2015') }}
WHERE borough = 'manhattan'