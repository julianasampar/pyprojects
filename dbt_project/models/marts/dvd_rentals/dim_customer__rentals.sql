-- This solution is written for SQLite querying

/*
    This dimension aims to treat customers data.
    The values for creation_date are posterior to the actual rental dates,
    which doesn't make sense. Here, we get the date of the first rental and 
    assign it as being the customer's creation date.
*/
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    MIN(DATE(rental_date)) AS created_date
FROM dvd_rental_store__customer
LEFT JOIN dvd_rental_store__rental USING (customer_id)
GROUP BY 
    customer_id,
    first_name,
    last_name,
    email