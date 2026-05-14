
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select indicator_value
from main."fct_nyc_air_quality"
where indicator_value is null



  
  
      
    ) dbt_internal_test