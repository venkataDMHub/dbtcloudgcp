{{ config(materialized='view') }}

with user_phone as (
    select user_id,
           identifier as phone_number
    from {{ var('core_public') }}.contacts
    where type = 'phone'
), 
type_phone as (
  select value as phone_number,
         reason as risk_reason
  from {{ var('compliance_public') }}.blacklisted_datapoints
  where type in ('PHONE_NUMBER', 'MOBILE_MONEY')
    and removed = FALSE 
)

select distinct u.user_id,
       'LRC_BAD_ASSETS' as risk_type,
        t.risk_reason      
from type_phone t join user_phone u  
    on u.phone_number = t.phone_number
