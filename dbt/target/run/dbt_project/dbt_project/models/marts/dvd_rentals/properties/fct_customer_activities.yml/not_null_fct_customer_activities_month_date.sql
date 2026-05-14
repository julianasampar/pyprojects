
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select month_date
from main."fct_customer_activities"
where month_date is null



  
  
      
    ) dbt_internal_test