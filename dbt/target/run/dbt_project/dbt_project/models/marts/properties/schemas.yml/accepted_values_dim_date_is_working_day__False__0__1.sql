
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_working_day as value_field,
        count(*) as n_records

    from main."dim_date"
    group by is_working_day

)

select *
from all_values
where value_field not in (
    0,1
)



  
  
      
    ) dbt_internal_test