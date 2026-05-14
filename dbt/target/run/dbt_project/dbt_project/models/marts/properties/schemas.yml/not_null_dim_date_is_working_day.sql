
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select is_working_day
from main."dim_date"
where is_working_day is null



  
  
      
    ) dbt_internal_test