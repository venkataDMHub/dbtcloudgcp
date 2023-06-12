{{ config(
    materialized='table',
    schema='intermediate'
) }}
select 
    chargeback_id,
    decline_api_response,
    created_at
from
    utils.chargeback_audit_declines
qualify row_number() over(partition by chargeback_id order by created_at desc) = 1
