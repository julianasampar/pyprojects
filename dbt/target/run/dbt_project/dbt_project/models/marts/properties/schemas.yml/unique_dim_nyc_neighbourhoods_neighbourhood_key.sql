
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    neighbourhood_key as unique_field,
    count(*) as n_records

from main."dim_nyc_neighbourhoods"
where neighbourhood_key is not null
group by neighbourhood_key
having count(*) > 1



  
  
      
    ) dbt_internal_test