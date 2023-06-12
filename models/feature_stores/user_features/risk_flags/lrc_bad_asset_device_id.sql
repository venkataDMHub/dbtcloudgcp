{{ config(materialized='view') }}


with user_device_id as (
    select user_id,
           device_id
    from {{ var('core_public') }}.user_device_ids
), 
type_device_id as (
  select value as device_id,
         reason as risk_reason
  from {{ var('compliance_public') }}.blacklisted_datapoints
  where type = 'DEVICE_ID'
    and removed = FALSE 
)

select distinct u.user_id,
       'LRC_BAD_ASSETS' as risk_type,
        t.risk_reason      
from type_device_id t join user_device_id u  
    on u.device_id = t.device_id
order by user_id
