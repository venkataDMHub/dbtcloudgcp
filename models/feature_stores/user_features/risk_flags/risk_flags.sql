{{ config(materialized='table') }}

with risk_flags_combined as (
    {{ dbt_utils.union_relations(
        relations=[ref('referral_flags'), 
                   ref('device_flags'), 
                   ref('lrc_blocking_flags'),
                   ref('lrc_bad_asset_phone_number'),
                   ref('lrc_bad_asset_email_address'),
                   ref('lrc_bad_asset_kyc'),
                   ref('lrc_bad_asset_device_id'),
                   ref('nysm_similar_selfie_flags'),
                   ref('fraud_activities')
                   ]
    ) }}
)

select user_id, 
       risk_type,
       risk_reason
from risk_flags_combined
