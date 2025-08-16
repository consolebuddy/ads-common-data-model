## Adding a new ad platform

1. **Create/Load seed**
   - Add a CSV in `seeds/` named like `src_ads_<platform>_all_data.csv`
   - Run `dbt seed`.

2. **Create staging model**
   - Add `models/staging/stg_<platform>.sql` that selects from the new seed and maps fields to:
     - `channel` (set to '<platform>')
     - `date` (DATE)
     - `campaign_name`, `adgroup_name`, `ad_name`
     - `spend` (FLOAT), `clicks` (INT), `impressions` (INT), `engagements` (INT), `conversions` (INT)
   - Update `models/staging/staging.yml` with not_null tests on `channel`/`date`.

3. **Union into fact**
   - Open `models/marts/ads_basic_performance.sql` and add:
     ```
     union all
     select * from {{ ref('stg_<platform>') }}
     ```
   - Run `dbt run --select ads_basic_performance`.

4. **Validate**
   - Check CPC, conversion cost, etc. with the sanity SQL in the README.

5. **Refresh Looker Studio**
   - If using the same table, charts auto-refresh. Otherwise, re-point the data source.
