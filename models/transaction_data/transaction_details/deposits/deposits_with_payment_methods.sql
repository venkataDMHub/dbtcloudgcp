{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}
select
    deposits.id as hlo_id,
    deposits.linked_account_id,
    deposits.charge_id,
    deposits.card_charge_id,
    deposits.deposit_receipt_webhook_id,
    deposits.transfer_id,
    deposits.reverse_transfer_id,
    deposits.error_message,
    deposits.note,
    deposits.admin_id,
    deposits.journal_id,
    deposits.details,
    deposits.created_at,
    deposits.updated_at,
    deposits.status,
    deposits.user_id,
    payment_methods.payment_method_details
from chipper.{{ var("core_public") }}."DEPOSITS"
left join {{ref('payment_methods')}} as payment_methods
    on deposits.linked_account_id = payment_methods.linked_account_id
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where updated_at >= (select max(updated_at) from {{ this }})
{% endif %}
