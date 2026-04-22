-- This solution is written for SQLite querying
{{config(
    tags=['dvd_rentals']
)}}

/*
    This dimension aims to treat movies data.
    We don't have consistent information about when the movie was included
        in the business. For that reason, we get the date of the first rental and 
    assign it as being the film's purchase date.
*/
SELECT 
    films.film_id,
    films.title,
    films.rental_duration,
    films.rental_rate,
    films.replacement_cost,
    MIN(rentals.rental_date) AS purchase_date
FROM dvd_rental_store__film films
LEFT JOIN {{ ref('int_rentals') }} rentals USING (film_id)
GROUP BY 
    films.film_id,
    films.title,
    films.rental_duration,
    films.rental_rate,
    films.replacement_cost