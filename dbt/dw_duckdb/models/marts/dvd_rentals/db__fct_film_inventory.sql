-- This solution is written for DuckDB querying
{{config(
    tags=['dvd_rentals']
)}}

/*
    An inventory is a time-based view built on top of an initial static snapshot of the system.
    Here, we aim to replicate that.
    First, we define a reference date, in this case, the day before the first rental, when the
        inventory was fully stocked and under control.
    Next, we incorporate rental movements. The rental_date represents when a film leaves inventory,
        while the return_date indicates when it is added back. We are assuming that there was no
        movies replaced during this period of time.
    To accurately calculate the number of films available at any given point in time, we need a
        reference from it's prior state. For this reason, we apply a rolling sum over these movements.
    Observe that there are movies rented and returned in the same day. To log each individual movement,
         we must calculate the running sum using the rental timestamp date.
*/

WITH min_date AS (
    SELECT
        DATE_SUB(MIN(rental_timestamp), INTERVAL 1 day) AS day0_date
    FROM {{ ref('db__int_rentals') }}
)
, day0_log AS (
    SELECT
        day0_date as inventory_date, 
        film_id,
        store_id,
        COUNT(*) AS storage_volume
    FROM {{ ref('db__inventory') }}
    CROSS JOIN min_date
    GROUP BY 
        film_id,
        store_id,
        inventory_date
),
inventory_decrease AS (
    SELECT 
        rental_timestamp AS inventory_date,
        film_id,
        store_id,
        COUNT(DISTINCT rental_id)*-1 AS storage_movement
    FROM {{ ref('db__int_rentals') }}
    GROUP BY rental_timestamp,
            film_id,
            store_id
),
inventory_increase AS (
    SELECT 
        return_timestamp AS inventory_date,
        film_id,
        store_id,
        COUNT(DISTINCT rental_id) AS storage_movement
    FROM {{ ref('db__int_rentals') }}
    WHERE return_date IS NOT NULL
    GROUP BY return_timestamp,
            film_id,
            store_id
),
union_all AS (
    SELECT
        {{ generate_surrogate_key(['inventory_date', 'film_id', 'store_id', 0]) }} AS inventory_movement_id,
        *
    FROM day0_log
    UNION ALL
    SELECT
        {{ generate_surrogate_key(['inventory_date', 'film_id', 'store_id', -1]) }} AS inventory_movement_id,
        *
    FROM inventory_decrease
    UNION ALL
    SELECT
        {{ generate_surrogate_key(['inventory_date', 'film_id', 'store_id', 1]) }} AS inventory_movement_id,
        *
    FROM inventory_increase
)

SELECT
    inventory_movement_id,
    CAST(inventory_date AS DATE) AS inventory_date,
    film_id,
    store_id,
    SUM(storage_volume) OVER (
        PARTITION BY film_id, store_id ORDER BY inventory_date
        ) AS storage_volume
FROM union_all
