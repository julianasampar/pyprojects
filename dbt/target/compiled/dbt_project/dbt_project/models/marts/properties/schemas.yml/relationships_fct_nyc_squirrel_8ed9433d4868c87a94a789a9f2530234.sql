
    
    

with child as (
    select tree_species_key as from_field
    from main."fct_nyc_squirrels_by_trees"
    where tree_species_key is not null
),

parent as (
    select tree_species_key as to_field
    from main."dim_tree_species"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


