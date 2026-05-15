{{ config(materialized='table') }}

WITH species_cleaned AS (
    SELECT DISTINCT
        {{ standardize_nulls('species_common_name') }} AS species_common_name,
        {{ standardize_nulls('species_latin_name') }} AS species_latin_name,
        CASE 
            WHEN species_common_name LIKE '%oak%' OR species_latin_name LIKE 'quercus%'
                THEN 'oak family'
            WHEN species_common_name LIKE '%maple%' OR species_latin_name LIKE 'acer%'
                THEN 'maple family'  
            WHEN species_common_name LIKE '%pine%' OR species_latin_name LIKE 'pinus%'
                THEN 'pine family'
            WHEN species_common_name LIKE '%cherry%' OR species_latin_name LIKE 'prunus%'
                THEN 'cherry family'
            WHEN species_common_name LIKE '%linden%' OR species_latin_name LIKE 'tilia%'
                THEN 'linden family'
            WHEN species_common_name LIKE '%birch%' OR species_latin_name LIKE 'betula%'
                THEN 'birch family'
            WHEN species_common_name LIKE '%ash%' OR species_latin_name LIKE 'fraxinus%'
                THEN 'ash family'
            WHEN species_common_name LIKE '%elm%' OR species_latin_name LIKE 'ulmus%'
                THEN 'elm family'
            ELSE 'other'
        END AS tree_family,
        CASE 
            WHEN species_latin_name LIKE 'pinus%' 
                 OR species_latin_name LIKE 'picea%'
                 OR species_latin_name LIKE 'abies%'
                 OR species_common_name LIKE '%pine%'
                 OR species_common_name LIKE '%spruce%'
                 OR species_common_name LIKE '%fir%'
                 OR species_common_name LIKE '%cedar%'
                THEN 'coniferous'
            ELSE 'deciduous'  
        END AS tree_type
    FROM {{ ref('stg__nyc_street_tree_census__2015') }}
)

SELECT
    ROW_NUMBER() OVER (
            ORDER BY species_common_name,
                     species_latin_name
            ) AS tree_species_key,
    species_common_name,
    species_latin_name,
    tree_family,
    tree_type
FROM species_cleaned
WHERE species_common_name IS NOT NULL
    OR species_latin_name IS NOT NULL