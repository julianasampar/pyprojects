

    with
    
    all_values as (

        select 


            trim(neighbourhood_area) as neighbourhood_area
            
        from main."dim_nyc_neighbourhoods"

    ),

    errors as (

        select * from all_values
        where neighbourhood_area = ''

    )

    select * from errors

