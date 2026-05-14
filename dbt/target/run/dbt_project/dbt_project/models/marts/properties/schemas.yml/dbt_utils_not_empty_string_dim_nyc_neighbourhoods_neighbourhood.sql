
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


            trim(neighbourhood) as neighbourhood
            
        from main."dim_nyc_neighbourhoods"

    ),

    errors as (

        select * from all_values
        where neighbourhood = ''

    )

    select * from errors


  
  
      
    ) dbt_internal_test