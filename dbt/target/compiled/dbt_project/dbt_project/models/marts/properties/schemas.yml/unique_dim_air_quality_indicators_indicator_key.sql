
    
    

select
    indicator_key as unique_field,
    count(*) as n_records

from main."dim_air_quality_indicators"
where indicator_key is not null
group by indicator_key
having count(*) > 1


