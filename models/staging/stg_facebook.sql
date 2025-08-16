{{ config(materialized='view') }}

with src as (
  -- seed: src_ads_creative_facebook_all_data.csv
  select * from {{ ref('src_ads_creative_facebook_all_data') }}
),
renamed as (
  select
    cast(channel as string)                         as channel,
    cast(date as date)                              as date,
    cast(campaign_id as string)                     as campaign_name,
    cast(adset_id    as string)                     as adgroup_name,
    cast(creative_title as string)                  as ad_name,

    cast(spend as float64)                          as spend,
    cast(clicks as int64)                           as clicks,
    cast(impressions as int64)                      as impressions,

    -- Build "engagements" from common FB interaction signals
    (
      coalesce(cast(likes   as int64), 0) +
      coalesce(cast(shares  as int64), 0) +
      coalesce(cast(comments as int64), 0) +
      coalesce(cast(views   as int64), 0) +
      coalesce(cast(inline_link_clicks as int64), 0)
    )                                               as engagements,

    -- Use purchase count as the conversion (most universal FB action here)
    coalesce(cast(purchase as int64), 0)            as conversions
  from src
)
select * from renamed
