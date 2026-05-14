
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    measurement_key as unique_field,
    count(*) as n_records

from main."fct_nyc_air_quality"
where measurement_key is not null
group by measurement_key
having count(*) > 1



  
  
      
    ) dbt_internal_test