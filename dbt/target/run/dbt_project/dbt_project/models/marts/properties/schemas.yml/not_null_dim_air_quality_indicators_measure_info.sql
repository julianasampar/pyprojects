
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select measure_info
from main."dim_air_quality_indicators"
where measure_info is null



  
  
      
    ) dbt_internal_test