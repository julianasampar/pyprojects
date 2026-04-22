-- This solution is written for SQLite querying
{{config(
    tags=['dvd_rentals']
)}}

/* 
    An inventory is a time-based view built on top of an initial static snapshot of the system.
    Here, we aim to replicate that.
    First, we define a reference date, in this case, the day before the first rental, when the 
        inventory was fully stocked and under control.
    Next, we incorporate rental movements. The rental_date represents when a film leaves inventory, 
        while the return_date indicates when it is added back.
    To accurately calculate the number of films available at any given point in time, we need a 
        reference from it's prior state. For this reason, we apply a rolling sum over these movements.
*/

WITH min_date AS (
    SELECT 
        DATE(MIN(rental_date), '-1 day') AS day0_date
    FROM {{ ref('int_rentals') }}
)
, day0_log AS (
    SELECT
        day0_date as inventory_date, 
        film_id,
        store_id,
        COUNT(*) AS storage_volume
    FROM dvd_rental_store__inventory
    CROSS JOIN min_date
    GROUP BY 
        film_id,
        store_id,
        inventory_date
),
inventory_decrease AS (
    SELECT 
        DATE(rental_date) AS inventory_date,
        film_id,
        store_id,
        COUNT(DISTINCT rental_id)*-1 AS storage_movement
    FROM {{ ref('int_rentals') }}
    GROUP BY rental_date,
            film_id,
            store_id
),
inventory_increase AS (
    SELECT 
        DATE(return_date) AS inventory_date,
        film_id,
        store_id,
        COUNT(DISTINCT rental_id) AS storage_movement
    FROM {{ ref('int_rentals') }}
    WHERE return_date IS NOT NULL
    GROUP BY rental_date,
            film_id,
            store_id
),
union_all AS (
    SELECT * FROM day0_log
    UNION ALL
    SELECT * FROM inventory_decrease
    UNION ALL
    SELECT * FROM inventory_increase
)

SELECT 
    inventory_date,
    film_id,
    store_id,
    SUM(storage_volume) OVER (
        PARTITION BY film_id, store_id ORDER BY inventory_date
        ) AS storage_volume
FROM union_all
