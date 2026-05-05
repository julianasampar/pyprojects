SELECT 
    payment.amount as revenue,
    payment.amount - 5 as net_profit
FROM dvd_rental_store__payment