
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    rental_id as unique_field,
    count(*) as n_records

from main."fct_rental_profits"
where rental_id is not null
group by rental_id
having count(*) > 1



  
  
      
    ) dbt_internal_test