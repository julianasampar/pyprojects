{{ config(materialized='table') }}

/*SELECT 
   squirrels.latitude,
   trees.latitude,
   squirrels.longitude,
   trees.longitude
FROM {{ ref('stg__nyc_central_park_squirrels_census__2018') }} squirrels
INNER JOIN {{ ref('stg__nyc_street_tree_census__2015') }} trees ON 
   ROUND(squirrels.latitude, 3) = ROUND(trees.latitude, 3)
   AND ROUND(squirrels.longitude, 3) = ROUND(trees.longitude, 3)*/

   SELECT
      DISTINCT neighbourhood
   FROM {{ ref('stg__nyc_street_tree_census__2015') }} trees
;