

    with
    
    all_values as (

        select 


            trim(measure_info) as measure_info
            
        from main."dim_air_quality_indicators"

    ),

    errors as (

        select * from all_values
        where measure_info = ''

    )

    select * from errors

