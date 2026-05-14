

    with
    
    all_values as (

        select 


            trim(indicator_substance) as indicator_substance
            
        from main."dim_air_quality_indicators"

    ),

    errors as (

        select * from all_values
        where indicator_substance = ''

    )

    select * from errors

