{{ config(materialized='table') }}

SELECT 

FROM {{ ref('stg_nyc_air_quality__historical') }}