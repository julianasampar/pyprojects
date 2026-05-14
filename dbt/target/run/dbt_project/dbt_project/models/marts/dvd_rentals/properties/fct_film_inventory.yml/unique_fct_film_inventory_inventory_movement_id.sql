
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    inventory_movement_id as unique_field,
    count(*) as n_records

from main."fct_film_inventory"
where inventory_movement_id is not null
group by inventory_movement_id
having count(*) > 1



  
  
      
    ) dbt_internal_test