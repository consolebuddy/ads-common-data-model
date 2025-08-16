{{ config(materialized='view') }}

with src as (
  select * from {{ ref('src_ads_tiktok_ads_all_data') }}
)
, renamed as (
  select
    'tiktok'                            as channel,
    cast(stat_time_day as date)         as date,             -- adjust if different
    campaign_name,
    adgroup_name,
    ad_name,
    cast(spend as float64)              as spend,
    cast(clicks as int64)               as clicks,
    cast(impressions as int64)          as impressions,
    cast(engagements as int64)          as engagements,      -- if absent, 0
    cast(conversions as int64)          as conversions
  from src
)
select * from renamed;
