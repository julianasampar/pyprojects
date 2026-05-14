
    
    

select
    neighbourhood_area as unique_field,
    count(*) as n_records

from main."dim_nyc_neighbourhoods"
where neighbourhood_area is not null
group by neighbourhood_area
having count(*) > 1


