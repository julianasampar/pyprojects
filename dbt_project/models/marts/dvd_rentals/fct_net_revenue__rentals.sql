-- This solution is written for SQLite querying

/* 
    In the data, not every rental has a corresponding payment. For these cases, I'm assigning 
        revenue date as being the rental date.
    In addition, for rentals RETURNED without a corresponding payment, I'm assuming that the 
        payment was made, but not registered, and assigning the expected amount as being the revenue.
    For rentals NOT RETURNED and without a corresponding payment, zero as revenue and as expected revenue.
    
    To calculate expected revenue amount we should divide the quantity of days the movie was rented 
        by the rate duration (available in the film dataset). Then, multiply by the rental rate. 
    That way, we have the expected price of the rental.
    Looking at the data, we see that the rental rate is the minimum fee, so we should treat the data 
        to consider that in the expected revenue as well. 
*/

WITH revenue AS (
    SELECT 
        rental.rental_id,
        inventory.inventory_id,
        rental.customer_id,
        inventory.film_id,
        inventory.store_id,
        DATE(COALESCE(payment.payment_date, rental.rental_date)) AS revenue_date,
        DATE(rental.return_date) AS return_date,
                -- using set here cause sqlite doesn't let me reference a calculation without repeating code :( 
                {% set days_booked = '(JULIANDAY(DATE(rental.return_date)) - JULIANDAY(DATE(rental.rental_date)))' %}
        {{ days_booked }} AS days_booked,
        CASE 
            WHEN {{ days_booked }} < film.rental_duration
                THEN film.rental_rate
            ELSE ROUND(( {{ days_booked }} / film.rental_duration ), 2) * film.rental_rate
        END AS expected_revenue,
        payment.amount AS payment_amount
    FROM dvd_rental_store__rental rental
    LEFT JOIN dvd_rental_store__payment payment USING (rental_id)
    LEFT JOIN dvd_rental_store__inventory inventory USING (inventory_id)
    LEFT JOIN dvd_rental_store__film film USING (film_id)
),
cost AS (
    -- For the cost, we must get the replacement cost of rentals NOT RETURNED.
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
                -- using set here cause sqlite doesn't let me reference a calculation without repeating code :( 
                {% set revenue_amount = 'ROUND(COALESCE(revenue.payment_amount, 
                                            CASE WHEN revenue.return_date IS NOT NULL THEN revenue.expected_revenue END,
                                            0), 2)' %}
    {{ revenue_amount }} AS revenue_amount,
    ROUND(COALESCE(revenue.expected_revenue, 0), 2) AS expected_revenue,
    ROUND(COALESCE(1 - ({{ revenue_amount }} / revenue.expected_revenue), 0), 2) AS discount_rate,
    ROUND(COALESCE(cost.cost_amount, 0), 2) AS cost_amount,
    ROUND({{ revenue_amount }} - COALESCE(cost.cost_amount, 0), 2) AS net_revenue_amount
FROM revenue
LEFT JOIN cost USING (rental_id)