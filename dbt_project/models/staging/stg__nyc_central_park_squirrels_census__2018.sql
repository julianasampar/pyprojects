{{ config(materialized='table') }}

SELECT 
    "Unique Squirrel Id" AS unique_squirrel_id,
    "X" AS latitude,
    "Y" AS longitude,
    "Hectare" AS hectare,
    "Shift" AS shift,
    CAST(
        SUBSTR(CAST("Date" AS STRING), -4) 
            || SUBSTR(CAST("Date" AS STRING), 0, 5)
    AS INT) AS observation_date,
    "Hectare Squirrel Number" AS hectare_squirrel_number,
    "Age" AS squirrel_age,
    "Primary Fur Color" AS primary_fur_color,
    "Highlight Fur Color" AS highligh_fur_color,
    "Combination of Primary and Highlight Color" AS combination_of_primary_and_highlight_color,
    "Color notes" AS color_notes,
    "Location" AS location,
    "Above Ground Sighter Measurement" AS above_ground_sighter_measurement,
    "Specific Location" AS specific_location,
    "Running" AS was_running,
    "Chasing" AS was_chasing,
    "Climbing" AS was_climbing,
    "Eating" AS was_eating,
    "Foraging" AS was_foraging,
    "Other Activities" AS other_activities,
    "Kuks" AS kuks,
    "Quaas" AS quaas,
    "Moans" AS moans,
    "Tall flags" AS tall_flags,
    "Tall twitches" AS tall_twitches,
    "Approaches" AS was_approaching,
    "Indifferent" AS was_indifferent,
    "Runds from" AS was_running_from,
    "Other Interactions" AS other_interactions,
    "Lat/Long" AS latitude_longitude
FROM nyc_central_park_squirrels_census__2018