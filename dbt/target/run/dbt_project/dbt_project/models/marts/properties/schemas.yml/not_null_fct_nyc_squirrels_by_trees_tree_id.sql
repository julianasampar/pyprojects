
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select tree_id
from main."fct_nyc_squirrels_by_trees"
where tree_id is null



  
  
      
    ) dbt_internal_test