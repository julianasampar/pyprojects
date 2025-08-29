{{ config(materialized='table') }}

SELECT
   squirrels.observation_date,
   trees.tree_id,
   neigh.neighbourhood_key,
   COUNT(DISTINCT unique_squirrel_id) AS qty_squirrels_observed,
   trees.tree_diameter,
   trees.is_tree_alive,
   trees.tree_on_curb,
   species.tree_species_key,
   trees.sidewalk_damaged,
   trees.damaged_roots,
   trees.damaged_trunk,
   trees.damaged_branches,
   SUM(CASE 
      WHEN shift = 'pm'
      THEN 1 ELSE 0
   END) AS squirrels_observed_at_pm,
   SUM(CASE 
      WHEN shift = 'am'
      THEN 1 ELSE 0
   END) AS squirrels_observed_at_am,
   SUM(squirrels.was_running) AS squirrels_running,
   SUM(squirrels.was_chasing) AS squirrels_chasing,
   SUM(squirrels.was_climbing) AS squirrels_climbing,
   SUM(squirrels.was_eating) AS squirrels_eating,
   SUM(squirrels.was_foraging) AS squirrels_foraging,
   SUM(squirrels.was_approaching) AS squirrels_approaching,
   SUM(squirrels.was_indifferent) AS squirrels_indifferent,
   SUM(squirrels.was_running_from) AS squirrels_running_from,
   CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('int_central_park_squirrels') }} squirrels
INNER JOIN {{ ref('int_manhattan_trees') }} trees USING (latitude, longitude)
LEFT JOIN {{ ref('dim_nyc_neighbourhoods') }} neigh USING (neighbourhood)
LEFT JOIN {{ ref('dim_tree_species') }} species USING (species_common_name)
GROUP BY 
   trees.neighbourhood,
   trees.tree_id,
   squirrels.observation_date,
   trees.tree_diameter,
   trees.is_tree_alive,
   trees.tree_on_curb,
   species.tree_species_key,
   trees.sidewalk_damaged,
   trees.damaged_roots,
   trees.damaged_trunk,
   trees.damaged_branches