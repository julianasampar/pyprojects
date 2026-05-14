
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select week_in_year
from main."dim_date"
where week_in_year is null



  
  
      
    ) dbt_internal_test