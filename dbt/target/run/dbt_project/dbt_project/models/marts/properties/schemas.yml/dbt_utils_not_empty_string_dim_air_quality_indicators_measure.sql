
    select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
  

    with
    
    all_values as (

        select 


            trim(measure) as measure
            
        from main."dim_air_quality_indicators"

    ),

    errors as (

        select * from all_values
        where measure = ''

    )

    select * from errors


  
  
      
    ) dbt_internal_test