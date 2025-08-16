{{ config(materialized='view') }}

with src as (
  -- seed: src_ads_bing_all_data.csv
  select * from {{ ref('src_ads_bing_all_data') }}
),
renamed as (
  select
    cast(channel as string)                                           as channel,
    cast(date as date)                                                as date,
    cast(campaign_id as string)                                       as campaign_name,   -- ids used as names
    cast(adset_id    as string)                                       as adgroup_name,
    -- Prefer human label if available, else description, else the id
    coalesce(nullif(trim(concat(title_part_1, ' ', title_part_2)), ''),
             ad_description,
             cast(ad_id as string))                                   as ad_name,

    cast(spend as float64)                                            as spend,
    cast(clicks as int64)                                             as clicks,
    cast(imps as int64)                                               as impressions,

    -- No explicit engagement metric in this seed -> set to 0 (matches the reference chart)
    cast(0 as int64)                                                  as engagements,

    -- Provided as "conv"
    cast(conv as int64)                                               as conversions
  from src
)
select * from renamed
