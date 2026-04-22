-- This solution is written for SQLite querying
{{config(
    tags=['dvd_rentals']
)}}

/* The only purpose of this model is to centralize common treatments 
    and joins relevant for the analysis
    */

SELECT 
    rentals.rental_id,
    DATE(rentals.rental_date) AS rental_date,
    rentals.inventory_id,
    inventory.store_id,
    films.film_id,
    rentals.customer_id,
    DATE(rentals.return_date) AS return_date,
    rentals.staff_id
FROM dvd_rental_store__rental rentals
LEFT JOIN dvd_rental_store__inventory inventory USING (inventory_id)
LEFT JOIN dvd_rental_store__film films USING (film_id)