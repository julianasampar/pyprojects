

    with
    
    all_values as (

        select 


            trim(indicator_name) as indicator_name
            
        from main."dim_air_quality_indicators"

    ),

    errors as (

        select * from all_values
        where indicator_name = ''

    )

    select * from errors

