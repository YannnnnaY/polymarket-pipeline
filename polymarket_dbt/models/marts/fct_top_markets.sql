with markets as (
    select * from {{ ref('stg_markets') }}
)

select
    market_id,
    question,
    category,
    volume,
    volume_24hr,
    liquidity,
    days_until_close,
    volume_to_liquidity_ratio,
    rank() over (order by volume desc)      as volume_rank,
    rank() over (order by volume_24hr desc) as volume_24hr_rank
from markets
order by volume desc
limit 100

