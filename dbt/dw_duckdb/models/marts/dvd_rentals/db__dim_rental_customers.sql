-- This solution is written for DuckDB querying
{{config(
    tags=['dvd_rentals']
)}}
/*
    This dimension aims to treat customers data.
    The values for creation_date are posterior to the actual rental dates,
    which doesn't make sense. Here, we get the date of the first rental and 
    assign it as being the customer's creation date.
*/
SELECT 
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customers.email,
    MIN(rentals.rental_date) AS created_date
FROM  {{ source('main', 'dvd_rental_store__customer') }} customers
LEFT JOIN {{ ref('db__int_rentals') }} rentals USING (customer_id)
GROUP BY 
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customers.email