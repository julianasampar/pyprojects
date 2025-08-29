{{ config(materialized='table') }}

SELECT 
    tree_id,
    block_id,
    CAST(
        SUBSTR(created_at, -4) 
            || SUBSTR(created_at, 0, 3)
            || SUBSTR(created_at, 4, 2)
    AS INT) AS observation_date,
    tree_dbh AS tree_diameter,
    stump_diam AS stump_diameter,
    LOWER(curb_loc) AS tree_curb_location,
    LOWER(TRIM(status)) AS tree_status,
    LOWER(health) AS tree_health,
    LOWER(spc_latin) AS species_latin_name,
    LOWER(spc_common) AS species_common_name,
    LOWER(steward) AS stewards_observed,
    LOWER(guards) AS presence_of_guards,
    {{ create_boolean_from_dict(
        { 
            'sidewalk': 'sidewalk_damaged',
            'root_stone': 'damaged_roots_by_paving_stones',
            'root_grate': 'damaged_roots_by_metal_grates',
            'root_other': 'damaged_roots_by_other',
            'trunk_wire': 'damaged_trunk_by_rope_or_wires',
            'trnk_light': 'damaged_trunk_by_lights',
            'trnk_other': 'damaged_trunk_by_other',
            'brch_light': 'damaged_branch_by_lights_or_wire',
            'brch_shoe': 'damaged_branch_by_shoes',
            'brch_other': 'damaged_branch_by_other',
        }
    ) }},
    LOWER(address) AS address,
    postcode,
    LOWER(zip_city) AS city,
    LOWER(borough) AS borough,
    LOWER(nta_name) AS neighbourhood,
    LOWER(state) AS state,
    latitude AS latitude,
    longitude AS longitude,
    CURRENT_TIMESTAMP AS updated_at
FROM nyc_street_tree_census__2015