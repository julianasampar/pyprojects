
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        quarter as value_field,
        count(*) as n_records

    from main."dim_date"
    group by quarter

)

select *
from all_values
where value_field not in (
    1,2,3,4
)



  
  
      
    ) dbt_internal_test