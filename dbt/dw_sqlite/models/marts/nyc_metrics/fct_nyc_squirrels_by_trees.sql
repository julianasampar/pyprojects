{{ config(materialized='table') }}

SELECT
   squirrels.observation_date,
   trees.tree_id,
   neigh.neighbourhood_key,
   COUNT(DISTINCT squirrels.unique_squirrel_id) AS qty_squirrels_observed,
   trees.tree_diameter,
   trees.is_tree_alive,
   trees.tree_on_curb,
   species.tree_species_key,
   trees.sidewalk_damaged,
   trees.damaged_roots,
   trees.damaged_trunk,
   trees.damaged_branches,
   SUM(COALESCE(squirrels.was_running, 0)) AS squirrels_running,
   SUM(COALESCE(squirrels.was_chasing, 0)) AS squirrels_chasing,
   SUM(COALESCE(squirrels.was_climbing, 0)) AS squirrels_climbing,
   SUM(COALESCE(squirrels.was_eating, 0)) AS squirrels_eating,
   SUM(COALESCE(squirrels.was_foraging, 0)) AS squirrels_foraging,
   SUM(COALESCE(squirrels.was_approaching, 0)) AS squirrels_approaching,
   SUM(COALESCE(squirrels.was_indifferent, 0)) AS squirrels_indifferent,
   SUM(COALESCE(squirrels.was_running_from, 0)) AS squirrels_running_from,
   SUM(COALESCE(squirrels.was_vocalizing, 0)) AS squirrels_vocalizing,
   COALESCE(
      squirrels.was_watching + 
      squirrels.was_sitting +
      squirrels.was_sleeping +
      squirrels.was_grooming +
      squirrels.was_digging +
      squirrels.was_nesting, 0
      ) + COUNT(DISTINCT other_activities)
   AS squirrels_in_other_activities,
   CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('int_central_park_squirrels') }} squirrels
INNER JOIN {{ ref('int_nyc_trees') }} trees USING (latitude, longitude)
LEFT JOIN {{ ref('dim_tree_species') }} species USING (species_common_name)
LEFT JOIN {{ ref('dim_nyc_neighbourhoods') }} neigh 
   ON neigh.neighbourhood_area = trees.neighbourhood
GROUP BY 
   squirrels.observation_date,
   trees.tree_id,
   neigh.neighbourhood_key,
   trees.tree_diameter,
   trees.is_tree_alive,
   trees.tree_on_curb,
   species.tree_species_key,
   trees.sidewalk_damaged,
   trees.damaged_roots,
   trees.damaged_trunk,
   trees.damaged_branches