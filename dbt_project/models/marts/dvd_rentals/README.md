### Instructions (in construction)

- The years of the data between tables are confusing, ignore them when performing analysis - consider only the months.
- You should treat creation_date of the table customers, because they don't make sense when compared to the rental activity.

### Instructions (in construction)

- The years of the data between tables are confusing, ignore them when performing analysis - consider only the months.
- You should treat creation_date of the table customers, because they don't make sense when compared to the rental activity.

### Onboarding Questions
(1) Suppose you are the DVD Rental Store manager, what is the first metric you would like to know? The profits, of course! Here, your first activity is to create a fct_net_profits that returns the net value of rentals by day.

#### What a solution looks like: 
The engineer calculates the revenue and the cost per day. Subtracts the cost into the revenue to get the net profit of the business operation. The profit can be obtained by connecting rentals data to payments data, whereas the cost can be calculated by identifying non-returned rentals and finding the replacement cost of the DVD. (Ignore for now other metrics like the cost of the employees and the maintenance of the store)

(2) Now, you might want to think about specific marketing campaigns targeting active and/or churned customers. For that, it would be interesting to understand the loyalty and rental activity of said customers. Create a fct_customer_activity and categorize the customer's activities into: 
Loyalty:
✻ Loyal customers: customers that rented at least one movie for more than 3 months in a row;
✻ Engaging customers: customers that rented at least one movie for 2 months in a row;
✻ Sporadic customers: customers that rented at least one movie for 1 month in a row;
Churn:
✻ Disengaging customers: customers that, after renting once, didn't rent any movie for 1 month in a row;
✻ At-risk customers: customers that, after renting once, didn't rent any movie for 2 months in a row;
✻ Churned customers: customers that, after renting once, didn't rent any movie for more than 3 months in a row.


#### What a solution looks like: 