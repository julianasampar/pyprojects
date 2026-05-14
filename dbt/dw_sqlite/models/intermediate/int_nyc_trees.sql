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
    tree_status,
    {{ create_boolean_from_dict(
        {'tree_status': 'is_tree_alive'},['alive']
    )}},
       {{ create_boolean_from_dict(
        {'tree_curb_location': 'tree_on_curb'},['oncurb']
    )}},
    species_common_name,
    species_latin_name,
    sidewalk_damaged,
    IIF(damaged_roots_by_paving_stones
        OR damaged_trunk_by_lights
        OR damaged_roots_by_other, 1, 0) AS damaged_roots,
    IIF(damaged_trunk_by_lights
        OR damaged_trunk_by_rope_or_wires
        OR damaged_trunk_by_other, 1, 0) AS damaged_trunk,
    IIF(damaged_branch_by_lights_or_wire
        OR damaged_branch_by_shoes
        OR damaged_branch_by_other, 1, 0) AS damaged_branches,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg__nyc_street_tree_census__2015') }}