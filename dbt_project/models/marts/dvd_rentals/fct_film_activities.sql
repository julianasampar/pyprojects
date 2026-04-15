-- This solution is written for SQLite querying
{{config(
    tags=['dvd_rentals']
)}}

    /* 
    Calculating the range of dates considering the start date as
        being the date for the first rental, and end date as the last
    */
WITH RECURSIVE dates(dimension_date) AS (
    VALUES((SELECT MIN(rental_date) FROM fct_rentals))
    UNION ALL
    SELECT date(dimension_date, '+1 day')
    FROM dates
    WHERE dimension_date < (SELECT MAX(rental_date) FROM fct_rentals)
),
    /* 
    Getting all films and their purchase date. 
    Activity should not be calculated prior to each films's
        purchase date.
    */
films AS (
    SELECT 
        film_id,
        purchase_date
    FROM dim_films
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
    FROM films
    CROSS JOIN dates
    LEFT JOIN fct_rentals rentals
        ON films.film_id = rentals.film_id
        AND dates.dimension_date = DATE(rentals.rental_date)
    WHERE dates.dimension_date >= films.purchase_date
),
monthly_activity AS (
SELECT 
    DATE(dimension_date, 'start of month') AS month_date,
    film_id,
    SUM(had_activity) AS rental_quantity,
    SUM(had_activity) > 0 AS had_activity
FROM daily_activity
GROUP BY month_date, film_id
),
retroactive_activity AS (
    /* 
    Calculating retroactive activity for last 3 months.
    For recent purchased films, applying coalesce and assuming value as zero.
    */
SELECT 
    month_date,
    film_id,
    had_activity AS had_activity_1m,
    COALESCE(
        LAG(had_activity, 1) OVER(PARTITION BY film_id ORDER BY month_date)
        , 0) AS had_activity_2m,
    COALESCE(
        LAG(had_activity, 2) OVER(PARTITION BY film_id ORDER BY month_date),
        0) AS had_activity_3m,
    rental_quantity AS rental_quantity_1m,
    COALESCE(
        LAG(rental_quantity, 1) OVER(PARTITION BY film_id ORDER BY month_date),
        0) AS rental_quantity_2m,
    COALESCE(
        LAG(rental_quantity, 2) OVER(PARTITION BY film_id ORDER BY month_date),
        0) AS rental_quantity_3m
FROM monthly_activity
)
SELECT 
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
