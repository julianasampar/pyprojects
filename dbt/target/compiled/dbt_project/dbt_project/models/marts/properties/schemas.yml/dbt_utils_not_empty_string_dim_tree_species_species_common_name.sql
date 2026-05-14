

    with
    
    all_values as (

        select 


            trim(species_common_name) as species_common_name
            
        from main."dim_tree_species"

    ),

    errors as (

        select * from all_values
        where species_common_name = ''

    )

    select * from errors

