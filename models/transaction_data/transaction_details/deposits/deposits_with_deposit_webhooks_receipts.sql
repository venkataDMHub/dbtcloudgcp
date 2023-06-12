{{ config(materialized='ephemeral') }}

select 
    deposits_with_payment_methods.transfer_id,
    'DEPOSITS' as hlo_table,
    deposit_webhooks_receipts.provider as external_provider,
    deposit_webhooks_receipts.external_transaction_id as external_provider_transaction_id,
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
    object_construct(
        'depositWebhooksReceiptId', deposit_webhooks_receipts.id,
        'amount', deposit_webhooks_receipts.amount::number(38,2),
        'currency', deposit_webhooks_receipts.currency,
        'paymentID', deposit_webhooks_receipts.payment_id,
        'externalTimestamp', deposit_webhooks_receipts.external_timestamp
    ) as deposit_webhooks_receipt_details,
    try_parse_json('NULL') as bank_charge_details,
    deposit_webhooks_receipts.request_body as external_provider_transaction_details    

from {{ref('deposits_with_payment_methods')}} as deposits_with_payment_methods
join "CHIPPER".{{ var("core_public") }}."DEPOSIT_WEBHOOKS_RECEIPTS"
    on deposits_with_payment_methods.deposit_receipt_webhook_id = deposit_webhooks_receipts.id
