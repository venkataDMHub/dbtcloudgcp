{{ config(materialized='ephemeral') }}

select
    invited_user_id as user_id,
    id as referral_id,
    concat('Referral ', status) as acquisition_source
from chipper.{{ var("core_public") }}.referrals
