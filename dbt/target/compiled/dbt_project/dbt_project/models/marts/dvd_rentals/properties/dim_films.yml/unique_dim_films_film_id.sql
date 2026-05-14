
    
    

select
    film_id as unique_field,
    count(*) as n_records

from main."dim_films"
where film_id is not null
group by film_id
having count(*) > 1


