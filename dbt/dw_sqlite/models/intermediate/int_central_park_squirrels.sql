{{ config(materialized='table') }}

{% set common_columns = '
   unique_squirrel_id,
   observation_date,
   ROUND(latitude, 3) AS latitude,
   ROUND(longitude, 3) AS longitude,
   highlight_fur_color,
   location,
   was_running,
   was_chasing,
   was_climbing,
   was_eating,
   was_foraging,
   was_approaching,
   was_indifferent,
   was_running_from,
   was_watching,
   was_sitting,
   was_sleeping,
   COALESCE(IIF(kuks + quaas + moans > 0, 1, 0), was_vocalizing) AS was_vocalizing,
   was_grooming,
   was_digging,
   was_nesting,
   other_activities
' %}

SELECT
   {{ common_columns }},
   CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg__nyc_central_park_squirrels_census__2018') }}

UNION ALL

SELECT
   {{ common_columns }},
   CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg__nyc_central_park_squirrels_census__2020') }}