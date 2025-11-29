{{ config(
    materialized = 'table'
) }}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "to_date('2010-01-01')",
        end_date   = "dateadd(year, 5, current_date())"
    ) }}

)

select
    row_number() over (order by date_day) as date_sk,
    date_day                               as full_date,
    year(date_day)                         as year,
    month(date_day)                        as month,
    day(date_day)                          as day,
    weekofyear(date_day)                   as week_of_year,
    dayofweek(date_day)                    as day_of_week,
    to_char(date_day, 'Mon')               as month_name,
    to_char(date_day, 'Dy')                as day_name
from date_spine;
