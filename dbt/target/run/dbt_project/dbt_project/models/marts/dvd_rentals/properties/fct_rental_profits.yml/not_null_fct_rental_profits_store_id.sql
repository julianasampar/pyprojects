
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    



select store_id
from main."fct_rental_profits"
where store_id is null



  
  
      
    ) dbt_internal_test