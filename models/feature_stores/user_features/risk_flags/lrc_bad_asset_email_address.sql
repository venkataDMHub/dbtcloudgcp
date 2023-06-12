{{ config(materialized='view') }}

with user_email as (
    select user_id,
           identifier as email_address
    from {{ var('core_public') }}.contacts
    where type = 'email'
), 
type_email as (
  select value as email_address,
         reason as risk_reason
  from {{ var('compliance_public') }}.blacklisted_datapoints
  where type = 'EMAIL_ADDRESS'
    and removed = FALSE 
)

select distinct u.user_id,
       'LRC_BAD_ASSETS' as risk_type,
        t.risk_reason      
from type_email t join user_email u  
    on u.email_address = t.email_address
