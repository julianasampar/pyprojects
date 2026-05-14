
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select measurement_key
from main."fct_nyc_air_quality"
where measurement_key is null



  
  
      
    ) dbt_internal_test