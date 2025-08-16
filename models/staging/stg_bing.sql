{{ config(materialized='view') }}

with src as (
  select * from {{ ref('src_ads_bing_all_data') }}
),
renamed as (
  select
    'bing'                               as channel,
    cast(`date` as date)                 as date,          -- backtick source column
    campaign_name,
    adgroup_name,
    ad_name,
    cast(cost as float64)                as spend,
    cast(clicks as int64)                as clicks,
    cast(impressions as int64)           as impressions,
    coalesce(cast(engagements as int64), 0)  as engagements,
    coalesce(cast(conversions as int64), 0)  as conversions
  from src
)
select * from renamed
