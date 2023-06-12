{{ config(materialized='ephemeral') }}

select 
    deposits_with_payment_methods.transfer_id,
    'DEPOSITS' as hlo_table,
    charges.provider as external_provider,
    charges.provider_id as external_provider_transaction_id,
    deposits_with_payment_methods.reverse_transfer_id,
    deposits_with_payment_methods.error_message,
    deposits_with_payment_methods.note as note,
    deposits_with_payment_methods.admin_id as admin_id,
    deposits_with_payment_methods.hlo_id,
    deposits_with_payment_methods.payment_method_details,
    deposits_with_payment_methods.details as other_deposit_details,
    deposits_with_payment_methods.created_at,
    deposits_with_payment_methods.updated_at,
    deposits_with_payment_methods.status,
    deposits_with_payment_methods.user_id,
    object_construct(
        'chargeId',coalesce(charges.id, try_parse_json('NULL')),
        'errorMessage',coalesce(charges.error_message, try_parse_json('NULL')),
        'refId', coalesce(charges.ref_id, try_parse_json('NULL')),
        'amount',coalesce(charges.amount::number(38,2), try_parse_json('NULL')),
        'externalId',coalesce(charges.external_id, try_parse_json('NULL')),
        'type',coalesce(charges.type, try_parse_json('NULL')),
        'carrier',coalesce(charges.carrier, try_parse_json('NULL')),
        'networkId',coalesce(charges.network_id, try_parse_json('NULL')),
        'currency', coalesce(charges.currency, try_parse_json('NULL')),
        'status', coalesce(charges.status,try_parse_json('NULL')),
        'mobileMoneyAccountNumber',coalesce(charges.mobile_money_account_number, try_parse_json('NULL')),
        'mobileMoneyAccountCountry',coalesce(charges.mobile_money_account_country, try_parse_json('NULL'))) as charge_details,
    try_parse_json('NULL') as card_charge_details,
    try_parse_json('NULL') as deposit_webhooks_receipt_details,
    try_parse_json('NULL') as bank_charge_details,
    charges.details as external_provider_transaction_details
from {{ref('deposits_with_payment_methods')}} as deposits_with_payment_methods
join "CHIPPER".{{ var("core_public") }}."CHARGES"
    on deposits_with_payment_methods.charge_id = charges.id
    or deposits_with_payment_methods.hlo_id = charges.chargeable_id 
where charges.type = 'DEPOSIT'
