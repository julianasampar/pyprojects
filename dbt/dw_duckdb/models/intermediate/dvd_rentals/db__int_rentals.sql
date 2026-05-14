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
    CAST(rentals.rental_date AS DATE) AS rental_date,
    rentals.inventory_id,
    inventory.store_id,
    films.film_id,
    rentals.customer_id,
    rentals.return_date AS return_timestamp,
    CAST(rentals.return_date AS DATE) AS return_date,
    rentals.staff_id
FROM read_csv_auto('/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/dvd_rental_store/rental.csv') rentals
LEFT JOIN read_csv_auto('/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/dvd_rental_store/inventory.csv') inventory USING (inventory_id)
LEFT JOIN read_csv_auto('/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/others/archive/dvd_rental_store/film.csv') films USING (film_id)