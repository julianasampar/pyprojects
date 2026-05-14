{{ config(materialized='table') }}

SELECT 
    "Unique Squirrel Id" AS unique_squirrel_id,
    "X" AS longitude,
    "Y" AS latitude,
    "Hectare" AS hectare,
    LOWER("Shift") AS shift,
    CAST(
        SUBSTR(CAST("Date" AS STRING), -4) 
            || SUBSTR(CAST("Date" AS STRING), 0, 5)
    AS INT) AS observation_date,
    "Hectare Squirrel Number" AS hectare_squirrel_number,
    LOWER("Age") AS squirrel_age,
    LOWER("Primary Fur Color") AS primary_fur_color,
    LOWER("Highlight Fur Color") AS highlight_fur_color,
    LOWER("Combination of Primary and Highlight Color") AS combination_of_primary_and_highlight_color,
    LOWER("Color notes") AS color_notes,
    LOWER("Location") AS location,
    "Above Ground Sighter Measurement" AS above_ground_sighter_measurement,
    LOWER("Specific Location") AS specific_location,
    "Running" AS was_running,
    "Chasing" AS was_chasing,
    "Climbing" AS was_climbing,
    "Eating" AS was_eating,
    "Foraging" AS was_foraging,
    "Other Activities" AS other_activities,
    "Kuks" AS kuks,
    "Quaas" AS quaas,
    "Moans" AS moans,
    LOWER("Tall flags") AS tall_flags,
    LOWER("Tall twitches") AS tall_twitches,
    "Approaches" AS was_approaching,
    "Indifferent" AS was_indifferent,
    "Runs from" AS was_running_from,
    LOWER("Other Interactions") AS other_interactions,

    -- 2020 fields non-existing in 2018 data
    NULL as neighbourhood,
    NULL as area_id,
    NULL as park_name,
    NULL as park_id,
    NULL as was_watching,
    NULL as was_sitting,
    NULL as was_sleeping,
    NULL as was_vocalizing,
    NULL as was_grooming,
    NULL as was_digging,
    NULL as was_nesting,
    NULL as interactions_with_humans,
    NULL as activities,
    CURRENT_TIMESTAMP AS updated_at
FROM nyc_central_park_squirrels_census__2018