{{ config(materialized='table') }}

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
    "Park Name" AS park_name,
    "Park ID" AS park_id,
    LOWER("Primary Fur Color") AS primary_fur_color,
    LOWER("Highlights in Fur Color") AS highlights_in_fur_color,
    LOWER("Color Notes") AS color_notes,
    LOWER("Location") AS location,
    "Above Ground (Height in Feet)" AS above_ground_sighter_measurement,
    LOWER("Specific Location") AS specific_location,
    LOWER("Activities") AS activities,
    LOWER("Interactions with Humans") AS interactions_with_humans,
    LOWER("Other Notes or Observations") AS other_notes_or_observations
FROM nyc_central_park_squirrels_census__2020