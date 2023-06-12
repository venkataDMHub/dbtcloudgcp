{{ config(materialized='view') }}
{% set ngn_to_usd_rate=0.0024 %}

select
    chargebacks.id,
    flw_to_chargeback.transfer_id,
    cast(chargebacks.amount as float) as amount,
    chargebacks.flw_ref,
    chargebacks.status,
    chargebacks.stage,
    chargebacks.comment,
    chargebacks.due_date,
    chargebacks.settlement_id,
    chargebacks.created_at,
    chargebacks.transaction_id,
    chargebacks.tx_ref,
    cast(chargebacks.amount as float) * {{ ngn_to_usd_rate }} as amount_in_usd,
    -- after 45 days, a declined chargeback is considered as "won" according to flutterwave
    chargebacks.due_date + interval '45 day' as due_date_45_days,
    case
        when
            status = 'declined' and chargebacks.due_date + interval '45 day' < current_timestamp then 'won'
        else status
    end as updated_status,
    case
        when
            status = 'declined' and chargebacks.due_date + interval '45 day' < current_timestamp then 'won - expiry of 45 day period'
        else status
    end as updated_status_reason
from
    chipper.utils.chargebacks
left join
    {{ ref('mapping_flutterwave_to_chipper_transactions') }} as flw_to_chargeback on
        chargebacks.flw_ref = flw_to_chargeback.flw_ref
