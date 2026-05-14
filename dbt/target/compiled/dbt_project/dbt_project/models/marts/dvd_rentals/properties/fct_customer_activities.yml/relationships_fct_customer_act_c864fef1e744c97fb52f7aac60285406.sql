
    
    

with child as (
    select customer_id as from_field
    from main."fct_customer_activities"
    where customer_id is not null
),

parent as (
    select customer_id as to_field
    from main."dim_rental_customers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


