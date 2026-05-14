
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select qty_squirrels_observed
from main."fct_nyc_squirrels_by_trees"
where qty_squirrels_observed is null



  
  
      
    ) dbt_internal_test