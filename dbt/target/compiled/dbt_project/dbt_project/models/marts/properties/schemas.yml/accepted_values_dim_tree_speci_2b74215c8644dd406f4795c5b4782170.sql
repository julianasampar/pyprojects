
    
    

with all_values as (

    select
        tree_type as value_field,
        count(*) as n_records

    from main."dim_tree_species"
    group by tree_type

)

select *
from all_values
where value_field not in (
    'coniferous','deciduous'
)


