
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        season as value_field,
        count(*) as n_records

    from main."fct_nyc_air_quality"
    group by season

)

select *
from all_values
where value_field not in (
    'winter','spring','summer','fall','annual'
)



  
  
      
    ) dbt_internal_test