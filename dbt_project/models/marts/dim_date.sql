{{ config(materialized='table') }}

with recursive calendar(date_day) as (
    select date('1970-01-01')
    union all
    select date(date_day, '+1 day')
    from calendar
    where date_day < '2100-12-31'
)
select
    -- Date key in the format yyyymmdd (ex: 20250825)
    cast(strftime('%Y%m%d', date_day) as int) as date_key,

    -- ISO format
    date_day as dimension_date,

    -- Year / quarter / month / day
    cast(strftime('%Y', date_day) as int) as year,
    cast((cast(strftime('%m', date_day) as int) + 2) / 3 as int) as quarter,
    cast(strftime('%m', date_day) as int) as month,
    cast(strftime('%d', date_day) as int) as day,

    -- Day of the week (0 = Sunday, 1 = Monday, ... 6 = Saturday)
    cast(strftime('%w', date_day) as int) as day_in_week,
    -- Name for the day of the week
    case cast(strftime('%w', date_day) as int)
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        else 'Saturday'
    end as weekday_name,

    -- Name for the month
    case cast(strftime('%m', date_day) as int)
        when 1 then 'January'
        when 2 then 'February'
        when 3 then 'March'
        when 4 then 'April'
        when 5 then 'May'
        when 6 then 'June'
        when 7 then 'July'
        when 8 then 'August'
        when 9 then 'September'
        when 10 then 'October'
        when 11 then 'November'
        else 'December'
    end as month_name,

    -- Abreviation for the month
    case cast(strftime('%m', date_day) as int)
        when 1 then 'Jan'
        when 2 then 'Feb'
        when 3 then 'Mar'
        when 4 then 'Apr'
        when 5 then 'May'
        when 6 then 'Jun'
        when 7 then 'Jul'
        when 8 then 'Aug'
        when 9 then 'Sep'
        when 10 then 'Oct'
        when 11 then 'Nov'
        else 'Dec'
    end as month_abbreviation,

    -- Day of the year (1–366)
    cast(strftime('%j', date_day) as int) as day_in_year,

    -- Week of the year (1–53, ISO)
    cast(strftime('%W', date_day) as int) + 1 as week_in_year,

    -- Flag if is business day (ex: 0=weekend, 1=business day)
    case 
        when cast(strftime('%w', date_day) as int) in (0,6) then 0
        else 1
    end as is_working_day

from calendar