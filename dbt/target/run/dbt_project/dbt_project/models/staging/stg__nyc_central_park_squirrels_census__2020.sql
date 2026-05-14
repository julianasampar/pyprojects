
  
    
    
    create  table main."stg__nyc_central_park_squirrels_census__2020"
    as
        





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
    
        CASE 
            WHEN
                    LOWER('Interactions with Humans') LIKE LOWER('%approaches%')
            THEN 1 
            ELSE 0 
        END AS was_approaching, 
        CASE 
            WHEN
                    LOWER('Interactions with Humans') LIKE LOWER('%indifferent%')
            THEN 1 
            ELSE 0 
        END AS was_indifferent, 
        CASE 
            WHEN
                    LOWER('Interactions with Humans') LIKE LOWER('%runs from%')
            THEN 1 
            ELSE 0 
        END AS was_running_from, 
        CASE 
            WHEN
                    LOWER('Interactions with Humans') LIKE LOWER('%watching%') OR 
                    LOWER('Interactions with Humans') LIKE LOWER('%watches%') OR 
                    LOWER('Interactions with Humans') LIKE LOWER('%staring%')
            THEN 1 
            ELSE 0 
        END AS was_watching
,
    
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%running%')
            THEN 1 
            ELSE 0 
        END AS was_running, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%chasing%')
            THEN 1 
            ELSE 0 
        END AS was_chasing, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%climbing%')
            THEN 1 
            ELSE 0 
        END AS was_climbing, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%eating%')
            THEN 1 
            ELSE 0 
        END AS was_eating, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%foraging%')
            THEN 1 
            ELSE 0 
        END AS was_foraging, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%sitting%') OR 
                    LOWER('Activities') LIKE LOWER('%lounging%') OR 
                    LOWER('Activities') LIKE LOWER('%resting%')
            THEN 1 
            ELSE 0 
        END AS was_sitting, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%sleeping%')
            THEN 1 
            ELSE 0 
        END AS was_sleeping, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%chattering%') OR 
                    LOWER('Activities') LIKE LOWER('%shouting%') OR 
                    LOWER('Activities') LIKE LOWER('%vocalization%')
            THEN 1 
            ELSE 0 
        END AS was_vocalizing, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%grooming%') OR 
                    LOWER('Activities') LIKE LOWER('%cleaning%')
            THEN 1 
            ELSE 0 
        END AS was_grooming, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%digging%')
            THEN 1 
            ELSE 0 
        END AS was_digging, 
        CASE 
            WHEN
                    LOWER('Activities') LIKE LOWER('%nesting%')
            THEN 1 
            ELSE 0 
        END AS was_nesting
,
    -- Preserving original text for future analysis
    LOWER("Activities") AS activities,
    LOWER("Interactions with Humans") AS interactions_with_humans,
    LOWER("Other Notes or Observations") AS other_interactions,

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

  