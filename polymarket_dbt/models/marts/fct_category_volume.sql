with markets as (
    select * from {{ ref('stg_markets') }}
)

select
    category,
    count(market_id)                        as total_markets,
    round(sum(volume), 2)                   as total_volume,
    round(sum(volume_24hr), 2)              as total_volume_24hr,
    round(avg(liquidity), 2)                as avg_liquidity,
    round(avg(volume_to_liquidity_ratio), 4) as avg_vol_liq_ratio,
    count(case when is_high_volume 
        then 1 end)                         as high_volume_markets
from markets
group by category
order by total_volume desc