

WITH prep_air_quality AS (
    SELECT 
        unique_id AS measurement_key,
        start_date,
        CASE 
            WHEN time_period NOT LIKE '%annual%'
            THEN LEAD(start_date, 1) 
                OVER(PARTITION BY indicator_id, geo_place_name ORDER BY start_date ASC)
            WHEN time_period LIKE '%annual%'
                THEN start_date + 10000
        END AS end_date,
        indicator_id AS indicator_key,
        indicator_name,
        measure,
        measure_info,
        TRIM(
                SUBSTR(geo_place_name, 0, INSTR(geo_place_name, '(')) 
            ) AS neighbourhood,
        SUBSTR(time_period, 0, INSTR(time_period, ' ')) AS season,
        ROUND(value, 3) AS indicator_value
    FROM main."stg_nyc_air_quality__historical"
)
SELECT 
    measurement_key,
    start_date,
    end_date,
    indicator_key,
    CASE
        WHEN LOWER(TRIM(indicator_name)) IN ('', ' ', 'null', 'unknown', 'n/a', 'none', 'na', 'nil', 'empty', 'blank', 'missing', 'undefined', 'not available', 'not applicable', 'no data', 'no value', '#n/a', '#null', 'void', 'absent')
        THEN NULL
        ELSE LOWER(TRIM(indicator_name))
    END
    
 AS indicator_name,
    CASE
        WHEN LOWER(TRIM(measure)) IN ('', ' ', 'null', 'unknown', 'n/a', 'none', 'na', 'nil', 'empty', 'blank', 'missing', 'undefined', 'not available', 'not applicable', 'no data', 'no value', '#n/a', '#null', 'void', 'absent')
        THEN NULL
        ELSE LOWER(TRIM(measure))
    END
    
 AS measure,
    CASE
        WHEN LOWER(TRIM(measure_info)) IN ('', ' ', 'null', 'unknown', 'n/a', 'none', 'na', 'nil', 'empty', 'blank', 'missing', 'undefined', 'not available', 'not applicable', 'no data', 'no value', '#n/a', '#null', 'void', 'absent')
        THEN NULL
        ELSE LOWER(TRIM(measure_info))
    END
    
 AS measure_info,
    COALESCE(CASE
        WHEN LOWER(TRIM(neighbourhood)) IN ('', ' ', 'null', 'unknown', 'n/a', 'none', 'na', 'nil', 'empty', 'blank', 'missing', 'undefined', 'not available', 'not applicable', 'no data', 'no value', '#n/a', '#null', 'void', 'absent')
        THEN NULL
        ELSE LOWER(TRIM(neighbourhood))
    END
    
, 'new york city') AS neighbourhood,
    COALESCE(CASE
        WHEN LOWER(TRIM(season)) IN ('', ' ', 'null', 'unknown', 'n/a', 'none', 'na', 'nil', 'empty', 'blank', 'missing', 'undefined', 'not available', 'not applicable', 'no data', 'no value', '#n/a', '#null', 'void', 'absent')
        THEN NULL
        ELSE LOWER(TRIM(season))
    END
    
, 'periodic') AS season,
    indicator_value
FROM prep_air_quality