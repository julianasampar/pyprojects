
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

select
    dimension_date as unique_field,
    count(*) as n_records

from main."dim_date"
where dimension_date is not null
group by dimension_date
having count(*) > 1



  
  
      
    ) dbt_internal_test