
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    film_activity_id as unique_field,
    count(*) as n_records

from main."fct_film_activities"
where film_activity_id is not null
group by film_activity_id
having count(*) > 1



  
  
      
    ) dbt_internal_test