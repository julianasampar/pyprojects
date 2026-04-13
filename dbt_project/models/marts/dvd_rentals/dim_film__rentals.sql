-- This solution is written for SQLite querying

/*
    This dimension aims to treat movies data.
    We don't have consistent information about when the movie was included
        in the business. For that reason, we get the date of the first rental and 
    assign it as being the film's purchase date.
*/
SELECT 
    film.film_id,
    film.title,
    film.rental_duration,
    film.rental_rate,
    film.replacement_cost,
    MIN(rentals.rental_date) AS purchase_date
FROM dvd_rental_store__film film
LEFT JOIN fct_rental__rentals rentals USING (film_id)
GROUP BY 
    film.film_id,
    film.title,
    film.rental_duration,
    film.rental_rate,
    film.replacement_cost