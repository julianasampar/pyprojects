{{ config(materialized='table') }}

SELECT 
    *
FROM nyc_street_tree_census__2015