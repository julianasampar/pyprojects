-- This solution is written for DuckDB querying
{{config(
    meta={'database': 'duckdb'},
    tags=['dvd_rentals']
)}}

-- depends_on: {{ ref('db__int_rentals') }}
{{ activity_generator(dimension='film', dimension_date='purchase_date', source='db__dim_films') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['month_date', 'film_id']) }} AS film_activity_id,
    month_date,
    film_id,
    CASE
        WHEN had_activity_1m AND had_activity_2m AND had_activity_3m
            THEN 'High-demand Movies'
        WHEN had_activity_1m AND had_activity_2m
            THEN 'Consistently booked Movies'
        WHEN had_activity_1m
            THEN 'Occasionally booked Movies'
        WHEN NOT had_activity_1m AND had_activity_2m
            THEN 'Low-demand Movies'
        WHEN NOT had_activity_1m AND NOT had_activity_2m AND had_activity_3m
            THEN 'Underperforming Movies'
        WHEN NOT had_activity_1m AND NOT had_activity_2m AND NOT had_activity_3m
            THEN 'Stale Movies'
    END AS film_activity,
    rental_quantity_1m,
    rental_quantity_1m + rental_quantity_2m + rental_quantity_3m AS rental_quantity_3m
FROM retroactive_activity
