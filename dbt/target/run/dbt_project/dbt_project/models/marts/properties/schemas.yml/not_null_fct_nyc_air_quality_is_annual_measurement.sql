
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select is_annual_measurement
from main."fct_nyc_air_quality"
where is_annual_measurement is null



  
  
      
    ) dbt_internal_test