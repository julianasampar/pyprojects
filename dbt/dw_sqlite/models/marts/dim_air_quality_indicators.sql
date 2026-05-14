{{ config(materialized='table') }}

SELECT 
    DISTINCT
        indicator_key,
        CASE 
            WHEN TRIM(indicator_name) = 'fine particles (pm 2.5)' 
                OR TRIM(indicator_name) = 'nitrogen dioxide (no2)'
                OR TRIM(indicator_name) = 'ozone (o3)'
                OR TRIM(indicator_name) LIKE '%outdoor air toxics%'
                THEN 'concentration'
            WHEN TRIM(indicator_name) LIKE '%deaths%' THEN 'deaths'
            WHEN TRIM(indicator_name) LIKE '%boiler emissions%' THEN 'boiler emissions'
            WHEN TRIM(indicator_name) LIKE '%miles traveled (cars)%' THEN 'miles traveled by cars'
            WHEN TRIM(indicator_name) LIKE '%miles traveled (trucks)%' THEN 'miles traveled by trucks'
            WHEN TRIM(indicator_name) = 'annual vehicle miles traveled' THEN 'miles traveled'
            WHEN TRIM(indicator_name) LIKE '%emergency%' 
                OR TRIM(indicator_name) LIKE '%hospitalizations%' THEN 'hospitalizations'
        END AS indicator_name,
        CASE 
            WHEN TRIM(indicator_name) LIKE '%pm2.5%' 
                OR TRIM(indicator_name) LIKE '%pm 2.5%' THEN 'pm2.5'
            WHEN TRIM(indicator_name) LIKE '%no2%' THEN 'no2'
            WHEN TRIM(indicator_name) LIKE '%ozone%' THEN 'o3'
            WHEN TRIM(indicator_name) LIKE '%so2%' THEN 'so2'
            WHEN TRIM(indicator_name) LIKE '%nox%' THEN 'nox'
            WHEN TRIM(indicator_name) LIKE '%vehicle%' THEN 'vehicle'
            WHEN TRIM(indicator_name) LIKE '%benzene%' THEN 'benzene'
            WHEN TRIM(indicator_name) LIKE '%formaldehyde%' THEN 'formaldehyde'
        END AS indicator_substance,
        CASE 
            WHEN measure LIKE '%estimated annual rate%' THEN 'estimated rate'
            WHEN measure LIKE '%annual average concentration%' THEN 'mean'
            ELSE measure
        END as indicator_measure,
        measure_info,
        {{ create_boolean_from_extract(
            'measure',
            { 'is_underage_indicator': ['under age 18']}
        ) }},
        {{ create_boolean_from_extract(
            'indicator_name',
            { 'is_asthma_indicator': ['asthma'],
             'is_cardiac_indicator': ['cardiac', 'cardio'],
            },
        ) }}
FROM {{ ref('int_air_quality') }}