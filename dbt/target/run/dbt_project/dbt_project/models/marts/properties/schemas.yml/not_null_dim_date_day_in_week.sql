
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select day_in_week
from main."dim_date"
where day_in_week is null



  
  
      
    ) dbt_internal_test