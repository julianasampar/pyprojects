

    with
    
    all_values as (

        select 


            trim(species_latin_name) as species_latin_name
            
        from main."dim_tree_species"

    ),

    errors as (

        select * from all_values
        where species_latin_name = ''

    )

    select * from errors

