{{ config(materialized='view') }}

with src as (
  -- seed: src_promoted_tweets_twitter_all_data.csv
  select * from {{ ref('src_promoted_tweets_twitter_all_data') }}
),
renamed as (
  select
    cast(channel as string)                         as channel,
    cast(date as date)                              as date,
    cast(campaign_id as string)                     as campaign_name,
    cast(null as string)                            as adgroup_name,   -- not available
    cast(text as string)                            as ad_name,        -- tweet text as the creative name

    cast(spend as float64)                          as spend,

    -- Prefer "clicks"; fall back to "url_clicks" if clicks is missing/zero
    case
      when safe_cast(clicks as int64) is null or safe_cast(clicks as int64) = 0
        then coalesce(cast(url_clicks as int64), 0)
      else cast(clicks as int64)
    end                                              as clicks,

    cast(impressions as int64)                      as impressions,

    coalesce(cast(engagements as int64),
             cast(likes as int64),
             cast(video_total_views as int64),
             0)                                      as engagements,

    -- No conversion metric in this seed
    cast(0 as int64)                                 as conversions
  from src
)
select * from renamed
