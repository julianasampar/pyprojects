
# dbt Hands on Project Guide

Hello,
You are probably a buddy or a newcomer that is working with the dbt Hands On project on DVD RENTAL store data. 
In the database you find information about the stores, customers and rented films.
The database contains the ERD (Entity Relationship Diagram) below.
This file contains instructions to accurately answer or evaluate the questions listed on the DA Onboarding. If you spot any error or suggestion, please let the Onboarding team know!



## Question 1: fct_rental_profits
#### Suppose you are the DVD Rental Store manager, what is the first metric you would like to know? The profits, of course! Here, your first activity is to create a fct_rental_profits that returns the net value of rentals by day.
Level of Difficulty: Low-Medium
#### Tips:
- Think about the granularity in the data that you as a business owner would like to see. With this fact, you might want to be able to answer questions like: What store is more profitable? Is there a more profitable movie? Which customers generate the most amount of revenue?
- Some metrics you might think about adding in the final table: revenue_amount, cost_amount, profit_amount, discount_rate.
- Explore the source tables to understand which transformations are needed to produce accurate (or near-accurate) metrics!

#### Criterias for the Solution:
The criterias below are listed in order from must have to nice to have.
- Engineer calculates profit amount by doing (revenue_amount - cost_amount);
- Engineer identifies that cost_amount should be calculated pulling 'replacement_cost' for unreplaced (return_date IS NULL) dvds, which is done joining rental, inventory and film tables.
- Engineer identifies that profit should be pulled from the column 'amount' from payments table.
- Engineer displays on the final table the revenue date, revenue amount, cost amount and profit amount.
- Engineer identifies that there are rentals without associated payments. Therefore, engineer pulls that from rentals and do a left join to payments.
- Engineer identifies that the minimum payment of a rental is the rental_rate, available in the film table.
- Engineer treats the payment amount for rentals made AND returned, but without a corresponding payment, by calculating an expected amount.
- Engineer calculates the expected amount by getting the length of the rental (days_booked = date_diff('day', rental_date, return_date)), dividing it by the rental_duration and multiplying the result for the rental_rate. Both 'rental_duration' and 'rental_rate' must be pulled from film table, which must be joined through inventory table.
- Engineer displays additional dimensions, such as customer_id, film_id, store_id.
- Engineer displays additional calculations, such as expected_revenue and discount_rate.
## Question 2: fct_customer_activities
#### Now, you might want to think about specific marketing campaigns targeting active and/or churned customers. For that, it would be interesting to understand the loyalty and rental activity of said customers. Create a fct_customer_activity and categorize the customer's activities into: 
#### Loyalty: 
#### - Loyal customers: customers that rented at least one movie for more than 3 months in a row;
#### - Engaging customers: customers that rented at least one movie for 2 months in a row;
#### - Sporadic customers: customers that rented at least one movie for 1 month in a row;
#### Churn:
#### - Disengaging customers: customers that, after renting once, didn't rent any movie for 1 month in a row;
#### - At-risk customers: customers that, after renting once, didn't rent any movie for 2 months in a row;
#### - Churned customers: customers that, after renting once, didn't rent any movie for more than 3 months in a row.

Level of Difficulty: Medium-High

#### Tips:
- You can choose the best format to show the data. The table can be a 'photograph' of a given moment or a monthly report.
- Since the data is old, use the month of the last rental date as reference. 
- For the bucket calculations, look retroactively in time to get each metric. You might need to use cross joins and a lot of window functions!


#### Criterias for the Solution: 
- Engineer gets range of date, with monthly or daily intervals. Ideally, engineer calculates range by getting the minimum and maximum rental date from rental table, but other ranges are acceptable.
- Engineer gets all registered customers from customers table.
- Engineer performs a cross join between range of dates and customers.
- Engineer joins the result to the rental table on customer_id and dates (range_date = rental_date). 
- Engineer creates a flag or an aggregation calculations that showcases if customer rented a film at each date of range. Some examples of calculations can be 'rental_id IS NOT NULL', 'COUNT(DISTINCT rental_id)', 'COUNT(DISTINCT rental_id) > 0' or similars. Evaluate the granularity to see which fits best.
- Engineer creates retroactive calculations using window functions such as LAG() to retrieve activity from the previous month and the month before that.
- Engineer creates loyalty and churn buckets correctly as described in the question statement.
- Engineer notices that the column 'create_date' in customers table is not consistent with what rental data is showing.
- Engineer filters out date ranges prior to the customer's creation date, and calculates the loyalty/churn buckets starting from it's first rental.


## Other Tips

- The years of the data between tables are confusing, ignore them when performing analysis - consider only the months.
- You should treat creation_date of the table customers, because they don't make sense when compared to the rental activity. Create a new dim_customers where the creation_date is the date of the first rental.