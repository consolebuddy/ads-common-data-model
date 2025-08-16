{{ config(materialized='view') }}

with src as (
  select * from {{ ref('src_ads_bing_all_data') }}
)
, renamed as (
  select
    'bing'                              as channel,
    cast(date as date)                  as date,              -- adjust if date field is named differently
    campaign_name,
    adgroup_name,
    ad_name,
    cast(cost as float64)               as spend,             -- example: Bing often has 'cost'
    cast(clicks as int64)               as clicks,
    cast(impressions as int64)          as impressions,
    cast(engagements as int64)          as engagements,       -- if not present, use 0
    cast(conversions as int64)          as conversions        -- if not present, map from actions/conv
  from src
)
select * from renamed;
