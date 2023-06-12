{{ config(materialized='view') }}

with user_kyc as (
  select owner_id as user_id,
         doc_number as kyc_number
  from {{ var('compliance_public') }}.kyc_documents
  where doc_type in ('BVN', 'DRIVERS_LICENSE', 'PASSPORT_CARD', 'PASSPORT')
), 
type_kyc as (
  select value as kyc_number,
         reason as risk_reason
  from {{ var('compliance_public') }}.blacklisted_datapoints
  where type in ('BVN_NUMBER', 'DRIVER_LICENSE_NUMBER', 'PASSPORT_NUMBER')
    and removed = FALSE 
)

select distinct u.user_id,
       'LRC_BAD_ASSETS' as risk_type,
        t.risk_reason      
from type_kyc t join user_kyc u  
    on u.kyc_number = t.kyc_number
