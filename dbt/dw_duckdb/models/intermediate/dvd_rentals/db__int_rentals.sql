-- This solution is written for DuckDB querying
{{config(
    tags=['dvd_rentals']
)}}

/* The only purpose of this model is to centralize common treatments 
    and joins relevant for the analysis
    */

SELECT 
    rentals.rental_id,
    rentals.rental_date AS rental_timestamp,
    DATE(rentals.rental_date) AS rental_date,
    rentals.inventory_id,
    inventory.store_id,
    films.film_id,
    rentals.customer_id,
    rentals.return_date AS return_timestamp,
    DATE(rentals.return_date) AS return_date,
    rentals.staff_id
FROM {{ ref('db__rental') }} rentals
LEFT JOIN {{ ref('db__inventory') }} inventory USING (inventory_id)
LEFT JOIN {{ ref('db__film') }} films USING (film_id)