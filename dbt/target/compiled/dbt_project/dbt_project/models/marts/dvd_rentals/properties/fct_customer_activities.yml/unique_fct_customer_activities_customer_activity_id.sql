
    
    

select
    customer_activity_id as unique_field,
    count(*) as n_records

from main."fct_customer_activities"
where customer_activity_id is not null
group by customer_activity_id
having count(*) > 1


