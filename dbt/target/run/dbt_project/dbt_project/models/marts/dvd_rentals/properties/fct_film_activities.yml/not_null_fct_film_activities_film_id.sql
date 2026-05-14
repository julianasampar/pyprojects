
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select film_id
from main."fct_film_activities"
where film_id is null



  
  
      
    ) dbt_internal_test