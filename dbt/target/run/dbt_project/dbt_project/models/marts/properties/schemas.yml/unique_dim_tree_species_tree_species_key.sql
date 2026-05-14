
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    tree_species_key as unique_field,
    count(*) as n_records

from main."dim_tree_species"
where tree_species_key is not null
group by tree_species_key
having count(*) > 1



  
  
      
    ) dbt_internal_test