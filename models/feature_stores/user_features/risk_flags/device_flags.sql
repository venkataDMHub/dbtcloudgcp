{{ config(materialized='view') }}

select distinct user_id,
       'DEVICE' as risk_type,
       'FLAGGED_BY_SARDINE_VERY_HIGH_RISK' as risk_reason
from {{ ref('ranked_sardine_device_ids') }}
where risk_level = 'very_high'
