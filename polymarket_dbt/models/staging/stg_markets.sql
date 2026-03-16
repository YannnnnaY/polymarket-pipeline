with source as (
    select * from raw_markets
)

select
    market_id,
    question,
    category,
    round(volume, 2)        as volume,
    round(volume_24hr, 2)   as volume_24hr,
    round(liquidity, 2)     as liquidity,
    cast(start_date as timestamp) as start_date,
    cast(end_date as timestamp)   as end_date,
    active,
    closed,
    days_until_close,
    round(volume_to_liquidity_ratio, 4) as volume_to_liquidity_ratio,
    is_high_volume,
    fetched_at
from source
where market_id is not null
  and volume >= 0