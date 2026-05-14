
  
    
    
    create  table main."int_nyc_trees"
    as
        

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
    CASE 
        WHEN LOWER(TRIM(tree_status)) IN ('alive') THEN 1
        WHEN LOWER(TRIM(tree_status)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  is_tree_alive
            ,
       CASE 
        WHEN LOWER(TRIM(tree_curb_location)) IN ('oncurb') THEN 1
        WHEN LOWER(TRIM(tree_curb_location)) IN ('no','false','0','nodamage') THEN 0
        ELSE 0
        END AS  tree_on_curb
            ,
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
FROM main."stg__nyc_street_tree_census__2015"

  