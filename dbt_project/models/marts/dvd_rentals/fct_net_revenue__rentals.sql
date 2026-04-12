WITH revenue AS (
    /* 
    In the data, not every rental has a corresponding payment. 
        For this cases, we assign revenue date as being the rental date.
     In addition, rentals made AND returned which don't have a corresponding payment, 
        we assume that the payment was made, but not registered, and assign the rental rate 
        as being the revenue.
    For rentals NOT RETURNED and without a corresponding payment, we assign zero as revenue.
    */
    SELECT 
        rental.rental_id,
        inventory.inventory_id,
        rental.customer_id,
        inventory.film_id,
        inventory.store_id,
       DATE(COALESCE(payment.payment_date, rental.rental_date)) AS revenue_date,
       COALESCE(payment.amount, 
                CASE WHEN rental.return_date IS NOT NULL THEN film.rental_rate END,
                0) AS revenue_amount,
        JULIANDAY(DATE(rental.return_date)) - JULIANDAY(DATE(rental.rental_date)) AS days_booked
    FROM dvd_rental_store__rental rental
    LEFT JOIN dvd_rental_store__payment payment USING (rental_id)
    LEFT JOIN dvd_rental_store__inventory inventory USING (inventory_id)
    LEFT JOIN dvd_rental_store__film film USING (film_id)
),
cost AS (
    /* 
        For the cost, we must get the replacement cost of rentals NOT RETURNED.
    */
    SELECT 
        rental.rental_id,
        film.replacement_cost AS cost_amount -- Replacement cost should not be NULL
    FROM dvd_rental_store__rental rental
    LEFT JOIN dvd_rental_store__inventory inventory USING (inventory_id)
    LEFT JOIN dvd_rental_store__film film USING (film_id)
    WHERE rental.return_date IS NULL
)
SELECT 
    rental_id,
    revenue.revenue_date,
    revenue.customer_id,
    revenue.film_id,
    revenue.store_id,
    ROUND(revenue.revenue_amount, 2) AS revenue_amount,
    ROUND(COALESCE(cost.cost_amount, 0), 2) AS cost_amount,
    ROUND(revenue.revenue_amount - COALESCE(cost.cost_amount, 0), 2) AS net_revenue_amount
FROM revenue
LEFT JOIN cost USING (rental_id)