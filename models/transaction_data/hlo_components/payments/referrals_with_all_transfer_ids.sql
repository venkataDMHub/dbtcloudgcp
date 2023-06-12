{{ config(materialized='ephemeral') }}

{# /* Transfer IDs of the referral bonuses for the referrers */ #}
select referrer_transfer_id as transfer_id
from "CHIPPER".{{ var("core_public") }}."REFERRALS"

union

{# /* Transfer IDs of the referral bonuses for the invited users */ #}
select invited_transfer_id as transfer_id
from "CHIPPER".{{ var("core_public") }}."REFERRALS"

union

{# /* Transfer IDs of the referral bonuses for the referrers when their invited friends referred somebody else 
    A referred B. B referred C. A gets a "secondary referral bonus" also after B referred C. */ #}
select secondary_transfer_id as transfer_id
from "CHIPPER".{{ var("core_public") }}."REFERRALS"
