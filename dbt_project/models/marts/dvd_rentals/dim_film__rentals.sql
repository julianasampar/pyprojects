-- This solution is written for SQLite querying

/*
    This dimension aims to treat movies data.
    We don't have consistent information about when the movie was included
        in the business. For that reason, we get the date of the first rental and 
    assign it as being the film's purchase date.
*/
SELECT 
    film_id,
    title,
    rental_duration,
    rental_rate,
    replacement_cost,
    MIN(DATE(rental_date)) AS purchase_date
FROM dvd_rental_store__film
LEFT JOIN dvd_rental_store__inventory USING (inventory_id)
LEFT JOIN dvd_rental_store__film USING (film_id)
GROUP BY 
    film_id,
    title,
    rental_duration,
    rental_rate,
    replacement_cost