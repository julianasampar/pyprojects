{{ config(materialized='table') }}

SELECT 
   *
FROM {{ ref('stg__nyc_central_park_squirrels_census__2018') }}