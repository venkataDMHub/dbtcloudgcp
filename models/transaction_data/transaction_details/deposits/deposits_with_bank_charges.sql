{{ config(materialized='ephemeral') }}

select
    deposits_with_payment_methods.transfer_id,
    'DEPOSITS' as hlo_table,
    case when cfsb_ach_tracking.bank_charge_id is not null then 'CFSB' else null end as external_provider, 
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
    object_construct(
        'bankChargeId',bank_charges.ID,
        'bankAccountID',bank_charges.bank_account_id,
        'amount',bank_charges.amount::number(38,2),
        'errorMessage',bank_charges.error_message,
        'chargeableType',bank_charges.chargeable_type,
        'currency',bank_charges.currency,
        'status',bank_charges.status
         ) as bank_charge_details,
    bank_charges.details as external_provider_transaction_details
from {{ref('deposits_with_payment_methods')}} as deposits_with_payment_methods
join chipper.{{ var("core_public") }}."BANK_CHARGES"
    on deposits_with_payment_methods.hlo_id = bank_charges.chargeable_id
left join chipper.{{ var("core_public") }}."CFSB_ACH_TRACKING"
    on cfsb_ach_tracking.bank_charge_id = bank_charges.id
where bank_charges.chargeable_type = 'DEPOSIT'
    and deposits_with_payment_methods.transfer_id not in (
        select transfer_id
        from {{ref('deposits_with_deposit_webhooks_receipts')}}
    )
