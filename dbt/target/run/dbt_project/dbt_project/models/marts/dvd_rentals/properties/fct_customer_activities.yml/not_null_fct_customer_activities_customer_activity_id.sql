
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select customer_activity_id
from main."fct_customer_activities"
where customer_activity_id is null



  
  
      
    ) dbt_internal_test