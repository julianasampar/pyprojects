{% macro activity_generator(dimension='', dimension_date='', source='') %}

WITH RECURSIVE dates(dimension_date) AS (
    VALUES((SELECT MIN(rental_date) FROM {{ ref('int_rentals') }}))
    UNION ALL
    SELECT date(dimension_date, '+1 day')
    FROM dates
    WHERE dimension_date < (SELECT MAX(rental_date) FROM {{ ref('int_rentals') }})
),
{{ dimension }} AS (
    SELECT 
        {{ dimension }}_id,
       {{ dimension_date }}
    FROM {{ source }}
),
daily_activity AS (
    /* 
    A cross join should be made between films and dates.
    Then, we join rentals to see if the movie was booked at each date.
    The daily aggregation is not necessary, but at the end of the code I'm 
        bringing the overall rental quantity as an extra, and for that we
        need to join by day.
    */
    SELECT 
        dates.dimension_date,
        films.film_id,
        rentals.rental_id IS NOT NULL AS had_activity
    FROM {{ dimension }}
    CROSS JOIN dates
    LEFT JOIN {{ ref('int_rentals') }} rentals
        ON {{ dimension }}.{{ dimension }}_id = rentals.{{ dimension }}
        AND dates.dimension_date = DATE(rentals.rental_date)
    WHERE dates.dimension_date >= {{ dimension }}.{{ dimension_date }}
),
monthly_activity AS (
SELECT 
    DATE(dimension_date, 'start of month') AS month_date,
    {{ dimension }}_id,
    SUM(had_activity) AS rental_quantity,
    SUM(had_activity) > 0 AS had_activity
FROM daily_activity
GROUP BY month_date, {{ dimension }}_id
),
retroactive_activity AS (
    /* 
    Calculating retroactive activity for last 3 months.
    For recent purchased films, applying coalesce and assuming value as zero.
    */
SELECT 
    month_date,
    {{ dimension }}_id,
    had_activity AS had_activity_1m,
    COALESCE(
        LAG(had_activity, 1) OVER(PARTITION BY {{ dimension }}_id ORDER BY month_date)
        , 0) AS had_activity_2m,
    COALESCE(
        LAG(had_activity, 2) OVER(PARTITION BY {{ dimension }}_id ORDER BY month_date),
        0) AS had_activity_3m,
    rental_quantity AS rental_quantity_1m,
    COALESCE(
        LAG(rental_quantity, 1) OVER(PARTITION BY {{ dimension }}_id ORDER BY month_date),
        0) AS rental_quantity_2m,
    COALESCE(
        LAG(rental_quantity, 2) OVER(PARTITION BY {{ dimension }}_id ORDER BY month_date),
        0) AS rental_quantity_3m
FROM monthly_activity
)
{% endmacro %}