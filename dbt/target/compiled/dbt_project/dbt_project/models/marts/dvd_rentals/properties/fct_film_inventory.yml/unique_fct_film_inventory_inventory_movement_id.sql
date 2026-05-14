
    
    

select
    inventory_movement_id as unique_field,
    count(*) as n_records

from main."fct_film_inventory"
where inventory_movement_id is not null
group by inventory_movement_id
having count(*) > 1


