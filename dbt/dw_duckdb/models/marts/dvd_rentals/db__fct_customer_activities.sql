-- This solution is written for DuckDB querying
{{config(
    tags=['dvd_rentals']
)}}

    /*
    Calculating the range of dates considering the start date as
        being the date for the first rental, and end date as the last
    */
WITH RECURSIVE dates(dimension_date) AS (
    SELECT MIN(rental_date) AS dimension_date FROM {{ ref('db__int_rentals') }}
    UNION ALL
    SELECT DATE_ADD(dimension_date, INTERVAL 1 day)
    FROM dates
    WHERE dimension_date < (SELECT MAX(rental_date) FROM {{ ref('db__int_rentals') }})
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
    FROM {{ ref('db__dim_rental_customers') }}
),
daily_activity AS (
    /* 
    A cross join should be made between customers and dates.
    Then, we join rentals to see if a movie was booked at each date.
    The daily aggregation is not necessary, but at the end of the code I'm 
        bringing the overall rental quantity as an extra, and for that we
        need to join by day.
    */
    SELECT 
        dates.dimension_date,
        customers.customer_id,
        rentals.rental_id IS NOT NULL AS had_activity
    FROM customers
    CROSS JOIN dates
    LEFT JOIN {{ ref('db__int_rentals') }} rentals
        ON customers.customer_id = rentals.customer_id
        AND dates.dimension_date = rentals.rental_date
    WHERE dates.dimension_date >= customers.created_date
),
monthly_activity AS (
SELECT
    DATE_TRUNC('month', dimension_date)::DATE AS month_date,
    customer_id,
    SUM(had_activity) AS rental_quantity,
    SUM(had_activity) > 0 AS had_activity
FROM daily_activity
GROUP BY month_date, customer_id
),
retroactive_activity AS (
    /* 
    Calculating retroactive activity for last 3 months.
    For recent customers, applying coalesce and assuming value as zero.
    */
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
    {{ dbt_utils.generate_surrogate_key(['month_date', 'customer_id']) }} AS customer_activity_id,
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
    rental_quantity_1m,
    rental_quantity_1m + rental_quantity_2m + rental_quantity_3m AS rental_quantity_3m
FROM retroactive_activity
