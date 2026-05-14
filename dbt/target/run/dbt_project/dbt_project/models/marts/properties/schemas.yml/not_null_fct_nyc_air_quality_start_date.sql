
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select start_date
from main."fct_nyc_air_quality"
where start_date is null



  
  
      
    ) dbt_internal_test