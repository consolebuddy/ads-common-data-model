{{ config(materialized='view') }}

with src as (
  -- seed: src_ads_tiktok_ads_all_data.csv  (note: file name says "add", not "all")
  select * from {{ ref('src_ads_tiktok_ads_all_data') }}
),
renamed as (
  select
    cast(channel as string)                         as channel,
    cast(date as date)                              as date,
    cast(campaign_id as string)                     as campaign_name,
    cast(adgroup_id  as string)                     as adgroup_name,
    cast(ad_text     as string)                     as ad_name,

    cast(spend as float64)                          as spend,
    cast(clicks as int64)                           as clicks,
    cast(impressions as int64)                      as impressions,

    -- Reference chart shows TikTok "Cost per engage" at ~0 (treat no engagements available)
    cast(0 as int64)                                as engagements,

    -- Prefer generic "conversions" if present; otherwise fall back to purchase
    coalesce(cast(conversions as int64),
             cast(purchase    as int64),
             0)                                       as conversions
  from src
)
select * from renamed
