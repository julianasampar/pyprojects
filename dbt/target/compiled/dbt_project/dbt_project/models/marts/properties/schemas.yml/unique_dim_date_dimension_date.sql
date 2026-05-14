
    
    

select
    dimension_date as unique_field,
    count(*) as n_records

from main."dim_date"
where dimension_date is not null
group by dimension_date
having count(*) > 1


