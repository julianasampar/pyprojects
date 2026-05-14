
    
    

select
    neighbourhood as unique_field,
    count(*) as n_records

from main."dim_nyc_neighbourhoods"
where neighbourhood is not null
group by neighbourhood
having count(*) > 1


