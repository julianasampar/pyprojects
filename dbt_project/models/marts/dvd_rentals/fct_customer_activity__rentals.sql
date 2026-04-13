-- This solution is written for SQLite querying

    /* 
    Calculating the range of dates considering the start date as
        being the date for the first rental, and end date as the last
    */
WITH RECURSIVE dates(dimension_date) AS (
    VALUES((SELECT MIN(DATE(rental_date)) FROM dvd_rental_store__rental))
    UNION ALL
    SELECT date(dimension_date, '+1 day')
    FROM dates
    WHERE dimension_date < (SELECT MAX(DATE(rental_date)) FROM dvd_rental_store__rental)
),
    /* 
    Getting all customers and their created date. 
    Activity should not be calculated prior to each customer's
        creation date.
    */
customers AS (
    SELECT 
        customer_id,
        created_date
    FROM dim_customer__rentals
),
daily_activity AS (
    /* 
    A cross join should be made between customers and dates.
    Then, we join rentals to see if a movie was booked at each date.
    */
    SELECT 
        dates.dimension_date,
        customers.customer_id,
        rentals.rental_id IS NOT NULL AS had_activity
    FROM customers
    CROSS JOIN dates
    LEFT JOIN dvd_rental_store__rental rentals
        ON customers.customer_id = rentals.customer_id
        AND dates.dimension_date = DATE(rentals.rental_date)
    WHERE dimension_date >= created_date
),
monthly_activity AS (
SELECT 
    DATE(dimension_date, 'start of month') AS month_date,
    customer_id,
    SUM(had_activity) AS rental_quantity,
    SUM(had_activity) > 0 AS had_activity
FROM daily_activity
GROUP BY month_date, customer_id
),
retroactive_activity AS (
SELECT 
    month_date,
    customer_id,
    had_activity AS had_activity_1m,
    COALESCE(
        LAG(had_activity, 1) OVER(PARTITION BY customer_id ORDER BY month_date)
        , 0) AS had_activity_2m,
    COALESCE(
        LAG(had_activity, 2) OVER(PARTITION BY customer_id ORDER BY month_date),
        0) AS had_activity_3m,
    rental_quantity AS rental_quantity_1m,
    COALESCE(
        LAG(rental_quantity, 1) OVER(PARTITION BY customer_id ORDER BY month_date),
        0) AS rental_quantity_2m,
    COALESCE(
        LAG(rental_quantity, 2) OVER(PARTITION BY customer_id ORDER BY month_date),
        0) AS rental_quantity_3m
FROM monthly_activity
)
SELECT 
    month_date,
    customer_id,
    CASE
        WHEN had_activity_1m AND had_activity_2m AND had_activity_3m
            THEN 'Loyal Customers'
        WHEN had_activity_1m AND had_activity_2m
            THEN 'Engaging Customers'
        WHEN had_activity_1m
            THEN 'Sporadic Customers'
        WHEN NOT had_activity_1m AND had_activity_2m
            THEN 'Disengaging Customers'
        WHEN NOT had_activity_1m AND NOT had_activity_2m AND had_activity_3m
            THEN 'At Risk Customers'
        WHEN NOT had_activity_1m AND NOT had_activity_2m AND NOT had_activity_3m
            THEN 'Churned Customers'
    END AS customer_activity,
    rental_quantity_1m + rental_quantity_2m + rental_quantity_3m AS rental_quantity_3m
FROM retroactive_activity
