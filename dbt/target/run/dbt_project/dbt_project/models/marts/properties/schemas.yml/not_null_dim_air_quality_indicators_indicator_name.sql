
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select indicator_name
from main."dim_air_quality_indicators"
where indicator_name is null



  
  
      
    ) dbt_internal_test