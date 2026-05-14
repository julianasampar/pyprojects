
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select neighbourhood_area
from main."dim_nyc_neighbourhoods"
where neighbourhood_area is null



  
  
      
    ) dbt_internal_test