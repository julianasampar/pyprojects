
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    customer_activity_id as unique_field,
    count(*) as n_records

from main."fct_customer_activities"
where customer_activity_id is not null
group by customer_activity_id
having count(*) > 1



  
  
      
    ) dbt_internal_test