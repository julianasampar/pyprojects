-- This solution is written for DuckDB querying
{{config(
    meta={'database': 'duckdb'},
    tags=['dvd_rentals']
)}}

/* The only purpose of this model is to centralize common treatments 
    and joins relevant for the analysis
    */

SELECT 
    rentals.rental_id,
    rentals.rental_date AS rental_timestamp,
    CAST(rentals.rental_date AS DATE) AS rental_date,
    rentals.inventory_id,
    inventory.store_id,
    films.film_id,
    rentals.customer_id,
    rentals.return_date AS return_timestamp,
    CAST(rentals.return_date AS DATE) AS return_date,
    rentals.staff_id
FROM {{ source('main', 'dvd_rental_store__rental') }} rentals
LEFT JOIN {{ source('main', 'dvd_rental_store__inventory') }} inventory USING (inventory_id)
LEFT JOIN {{ source('main', 'dvd_rental_store__film') }} films USING (film_id)