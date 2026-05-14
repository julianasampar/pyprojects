
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    film_id as unique_field,
    count(*) as n_records

from main."dim_films"
where film_id is not null
group by film_id
having count(*) > 1



  
  
      
    ) dbt_internal_test