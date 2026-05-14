

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

