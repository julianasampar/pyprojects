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
    LOWER(status) AS tree_status,
    LOWER(health) AS tree_health,
    LOWER(spc_latin) AS species_latin_name,
    LOWER(spc_common) AS species_common_name,
    LOWER(steward) AS stewards_observed,
    LOWER(guards) AS presence_of_guards,
    LOWER(sidewalk) AS sidewalk_damaged,
    LOWER(user_type) AS user_type,
    LOWER(root_stone) AS damaged_roots_by_paving_stones,
    LOWER(root_grate) AS damaged_roots_by_metal_grates,
    LOWER(root_other) AS damaged_roots_by_other,
    LOWER(trunk_wire) AS damaged_trunk_by_rope_or_wires,
    LOWER(trnk_light) AS damaged_trunk_by_lights,
    LOWER(trnk_other) AS damaged_trunk_by_other,
    LOWER(brch_light) AS damaged_branch_by_lights_or_wire,
    LOWER(brch_shoe) AS damaged_branch_by_shoes,
    LOWER(brch_other) AS damaged_branch_by_other,
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