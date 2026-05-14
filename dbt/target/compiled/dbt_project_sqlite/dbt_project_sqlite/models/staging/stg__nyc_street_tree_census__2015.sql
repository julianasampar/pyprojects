

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
    CASE 
        WHEN LOWER(TRIM(sidewalk)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(sidewalk)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  sidewalk_damaged
            ,CASE 
        WHEN LOWER(TRIM(root_stone)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(root_stone)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_roots_by_paving_stones
            ,CASE 
        WHEN LOWER(TRIM(root_grate)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(root_grate)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_roots_by_metal_grates
            ,CASE 
        WHEN LOWER(TRIM(root_other)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(root_other)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_roots_by_other
            ,CASE 
        WHEN LOWER(TRIM(trunk_wire)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(trunk_wire)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_trunk_by_rope_or_wires
            ,CASE 
        WHEN LOWER(TRIM(trnk_light)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(trnk_light)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_trunk_by_lights
            ,CASE 
        WHEN LOWER(TRIM(trnk_other)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(trnk_other)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_trunk_by_other
            ,CASE 
        WHEN LOWER(TRIM(brch_light)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(brch_light)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_branch_by_lights_or_wire
            ,CASE 
        WHEN LOWER(TRIM(brch_shoe)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(brch_shoe)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_branch_by_shoes
            ,CASE 
        WHEN LOWER(TRIM(brch_other)) IN ('yes','true','1','damage') THEN 1
        WHEN LOWER(TRIM(brch_other)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  damaged_branch_by_other
            ,
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