SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    MIN(DATE(rental_date)) AS created_date
FROM dvd_rental_store__customer
LEFT JOIN dvd_rental_store__rental USING (customer_id)
GROUP BY customer_id