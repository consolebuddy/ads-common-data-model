{{ config(materialized='view') }}

with src as (
  select * from {{ ref('src_ads_creative_facebook_all_data') }}
)
, renamed as (
  select
    'facebook'                          as channel,
    cast(date_start as date)            as date,            -- adjust to actual date column
    campaign_name,
    adset_name                          as adgroup_name,
    ad_name,
    cast(spend as float64)              as spend,           -- facebook typically uses 'spend'
    cast(clicks as int64)               as clicks,
    cast(impressions as int64)          as impressions,
    cast(engagement as int64)           as engagements,     -- sometimes 'engagement' or 'post_engagement'
    cast(purchases as int64)            as conversions      -- or map from 'conversions' object; else 0
  from src
)
select * from renamed
