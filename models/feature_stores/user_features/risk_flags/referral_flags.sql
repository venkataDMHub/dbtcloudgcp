{{ config(materialized='view') }}

(
    select distinct
        referrer_id as user_id,
        'REFERRAL' as risk_type,
        status as risk_reason
    from chipper.{{ var("core_public") }}.referrals
    where status like 'FLAGGED%'
)
union distinct
(
    select distinct
        invited_user_id as user_id,
        'REFERRAL' as risk_type,
        status as risk_reason
    from chipper.{{ var("core_public") }}.referrals
    where status like 'FLAGGED%'
)
