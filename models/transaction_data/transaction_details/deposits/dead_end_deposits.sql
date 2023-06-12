{{ config(materialized='ephemeral') }}

select
    deposits_with_payment_methods.transfer_id,
    'DEPOSITS' as hlo_table,
    null as external_provider,
    null as external_provider_transaction_id,
    deposits_with_payment_methods.reverse_transfer_id,
    deposits_with_payment_methods.error_message,
    deposits_with_payment_methods.note,
    deposits_with_payment_methods.admin_id,
    deposits_with_payment_methods.hlo_id,
    deposits_with_payment_methods.payment_method_details,
    deposits_with_payment_methods.details as other_deposit_details,
    deposits_with_payment_methods.created_at,
    deposits_with_payment_methods.updated_at,
    deposits_with_payment_methods.status,
    deposits_with_payment_methods.user_id,
    try_parse_json('NULL') as charge_details,
    try_parse_json('NULL') as card_charge_details,
    try_parse_json('NULL') as deposit_webhooks_receipt_details,
    try_parse_json('NULL') as bank_charge_details,
    try_parse_json('NULL') as external_provider_transaction_details
from {{ref('deposits_with_payment_methods')}} as deposits_with_payment_methods
where deposits_with_payment_methods.transfer_id not in (
    select transfer_id
    from {{ref('deposits_with_webhooks_and_charges')}})
