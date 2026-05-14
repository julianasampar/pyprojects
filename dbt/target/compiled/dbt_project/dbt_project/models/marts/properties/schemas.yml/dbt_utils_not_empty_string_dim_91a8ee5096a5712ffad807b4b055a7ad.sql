

    with
    
    all_values as (

        select 


            trim(indicator_measure) as indicator_measure
            
        from main."dim_air_quality_indicators"

    ),

    errors as (

        select * from all_values
        where indicator_measure = ''

    )

    select * from errors

