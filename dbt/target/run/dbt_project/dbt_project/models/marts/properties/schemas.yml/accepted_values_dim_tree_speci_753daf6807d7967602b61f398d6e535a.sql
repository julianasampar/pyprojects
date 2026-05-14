
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        tree_family as value_field,
        count(*) as n_records

    from main."dim_tree_species"
    group by tree_family

)

select *
from all_values
where value_field not in (
    'oak family','maple family','pine family','cherry family','linden family','birch family','ash family','elm family','other'
)



  
  
      
    ) dbt_internal_test