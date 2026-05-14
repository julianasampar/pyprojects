
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select tree_species_key
from main."dim_tree_species"
where tree_species_key is null



  
  
      
    ) dbt_internal_test