
    
    

with all_values as (

    select
        month_abbreviation as value_field,
        count(*) as n_records

    from main."dim_date"
    group by month_abbreviation

)

select *
from all_values
where value_field not in (
    'Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'
)


