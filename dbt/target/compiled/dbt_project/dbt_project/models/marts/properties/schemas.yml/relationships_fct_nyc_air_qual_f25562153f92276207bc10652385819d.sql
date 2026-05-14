
    
    

with child as (
    select neighbourhood_key as from_field
    from main."fct_nyc_air_quality"
    where neighbourhood_key is not null
),

parent as (
    select neighbourhood_key as to_field
    from main."dim_nyc_neighbourhoods"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


