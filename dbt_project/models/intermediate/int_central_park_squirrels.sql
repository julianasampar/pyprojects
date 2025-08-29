{{ config(materialized='table') }}

SELECT
   unique_squirrel_id,
   observation_date,
   ROUND(latitude, 3) AS latitude,
   ROUND(longitude, 3) AS longitude,
   shift,
   combination_of_primary_and_highlight_color,
   location,
   was_running,
   was_chasing,
   was_climbing,
   was_eating,
   was_foraging,
   other_activities,
   was_approaching,
   was_indifferent,
   was_running_from,
   other_activities,
   CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg__nyc_central_park_squirrels_census__2018') }}