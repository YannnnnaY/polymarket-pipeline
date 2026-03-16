with markets as (
    select * from {{ ref('stg_markets') }}
)

select
    category,
    count(case when days_until_close <= 7 
        then 1 end)     as closing_this_week,
    count(case when days_until_close between 8 and 30 
        then 1 end)     as closing_this_month,
    count(case when days_until_close > 30 
        then 1 end)     as closing_later,
    round(avg(days_until_close), 1) as avg_days_until_close
from markets
where days_until_close >= 0
group by category
order by closing_this_week desc