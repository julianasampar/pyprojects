

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

