{{ config(materialized='table') }}

with unioned as (
  select * from {{ ref('stg_bing') }}
  union all
  select * from {{ ref('stg_facebook') }}
  union all
  select * from {{ ref('stg_tiktok') }}
  union all
  select * from {{ ref('stg_twitter') }}
),
typed as (
  select
    cast(channel as string)                 as channel,
    cast(date as date)                      as date,
    coalesce(campaign_name, 'unknown')      as campaign_name,
    coalesce(adgroup_name,  'unknown')      as adgroup_name,
    coalesce(ad_name,       'unknown')      as ad_name,

    {{ zero_if_null('cast(spend as float64)') }}        as spend,
    {{ zero_if_null('cast(clicks as int64)') }}         as clicks,
    {{ zero_if_null('cast(impressions as int64)') }}    as impressions,
    {{ zero_if_null('cast(engagements as int64)') }}    as engagements,
    {{ zero_if_null('cast(conversions as int64)') }}    as conversions
  from unioned
)
select * from typed
