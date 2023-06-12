{{ config(materialized='ephemeral') }}

select 
    deposits_with_payment_methods.transfer_id,
    'DEPOSITS' as hlo_table,
    payment_cards.auth_token_issued_by as external_provider,
    payment_cards.auth_token as external_provider_transaction_id,
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
    object_construct(
        'cardChargeId',card_charges.id,
        'amount', card_charges.amount::number(38,2),
        'currency',card_charges.currency,
        'authToken',card_charges.auth_token,
        'type',card_charges.type,
        'status',card_charges.status
    ) as card_charge_details,
    try_parse_json('NULL') as deposit_webhooks_receipt_details,
    try_parse_json('NULL') as bank_charge_details,
    card_charges.details as external_provider_transaction_details
    
from {{ref('deposits_with_payment_methods')}} as deposits_with_payment_methods
join "CHIPPER".{{ var("core_public") }}."CARD_CHARGES"
    on (
    deposits_with_payment_methods.card_charge_id = card_charges.id
    or deposits_with_payment_methods.hlo_id = card_charges.chargeable_id
    )
left join "CHIPPER".{{ var("core_public") }}."PAYMENT_CARDS"
on payment_cards.auth_token = card_charges.auth_token
where card_charges.type = 'DEPOSIT'
