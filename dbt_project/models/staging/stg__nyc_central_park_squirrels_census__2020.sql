{{ config(materialized='table') }}

{% set interactions_with_humans = 'LOWER("Interactions with Humans")' %}
{% set activities = 'LOWER("Activities")' %}
{% set other_interactions = 'LOWER("Other Notes or Observations")' %}

SELECT
    "Squirrel ID" AS unique_squirrel_id,
    CAST(SUBSTR("Date", -4)
        || SUBSTR("Date", 4, 2)
        || SUBSTR("Date", 0, 3)
        AS INT) AS observation_date,
    "Squirrel Latitude (DD.DDDDDD)" AS latitude,
    "Squirrel Longitude (-DD.DDDDDD)" AS longitude,
    LOWER("Area Name") AS neighbourhood,
    "Area ID" AS area_id,
    LOWER("Park Name") AS park_name,
    "Park ID" AS park_id,
    LOWER("Primary Fur Color") AS primary_fur_color,
    LOWER("Highlights in Fur Color") AS highlight_fur_color,
    LOWER("Color Notes") AS color_notes,
    LOWER("Location") AS location,
    "Above Ground (Height in Feet)" AS above_ground_sighter_measurement,
    LOWER("Specific Location") AS specific_location,
    -- Extracting 2018-compatible behavioral flags
    {{ create_boolean_from_extract(
          "'Interactions with Humans'",
        { 'was_approaching': ['approaches'],
          'was_indifferent': ['indifferent'],
          'was_running_from': ['runs from'],
          'was_watching': ['watching', 'watches', 'staring'],
        }
    ) }},
    {{ create_boolean_from_extract(
        "'Activities'",
        { 'was_running': ['running'],
          'was_chasing': ['chasing'],
          'was_climbing': ['climbing'],
          'was_eating': ['eating'],
          'was_foraging': ['foraging'],
          'was_sitting': ['sitting', 'lounging', 'resting'],
          'was_sleeping': ['sleeping'],
          'was_vocalizing': ['chattering', 'shouting', 'vocalization'],
          'was_grooming': ['grooming', 'cleaning'],
          'was_digging': ['digging'],
          'was_nesting': ['nesting'],
        }
    ) }},
    -- Preserving original text for future analysis
    {{ activities }} AS activities,
    {{ interactions_with_humans }} AS interactions_with_humans,
    {{ other_interactions }} AS other_interactions,

    -- 2018 fields non-existing in 2020 data
    NULL as hectare,
    NULL as shift,
    NULL as hectare_squirrel_number,
    NULL as squirrel_age,
    NULL as combination_of_primary_and_highlight_color,
    NULL as other_activities,
    NULL as kuks,
    NULL as quaas,
    NULL as moans,
    NULL as tall_flags,
    NULL as tall_twitches,
    CURRENT_TIMESTAMP as updated_at
FROM nyc_central_park_squirrels_census__2020