{{ config(materialized='view') }}

with src as (
  select * from {{ ref('src_promoted_tweets_twitter_all_data') }}
)
, renamed as (
  select
    'twitter'                           as channel,
    cast(day as date)                   as date,             -- adjust to actual date field
    campaign_name,
    line_item_name                      as adgroup_name,
    tweet_name                          as ad_name,
    cast(billed_charge_local_micro/1e6 as float64) as spend, -- twitter cost often in micros
    cast(clicks as int64)               as clicks,
    cast(impressions as int64)          as impressions,
    cast(engagements as int64)          as engagements,
    cast(conversions as int64)          as conversions
  from src
)
select * from renamed;
