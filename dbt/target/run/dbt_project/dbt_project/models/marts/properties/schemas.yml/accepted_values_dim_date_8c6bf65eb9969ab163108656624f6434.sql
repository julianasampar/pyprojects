
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        month_name as value_field,
        count(*) as n_records

    from main."dim_date"
    group by month_name

)

select *
from all_values
where value_field not in (
    'January','February','March','April','May','June','July','August','September','October','November','December'
)



  
  
      
    ) dbt_internal_test