
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select squirrels_in_other_activities
from main."fct_nyc_squirrels_by_trees"
where squirrels_in_other_activities is null



  
  
      
    ) dbt_internal_test