{{ config(materialized='table') }}

select
    -- details about the chargebacks and related user
    distinct chargeback_with_transfer_id.id,
    expanded_ledgers.main_party_user_id as user_id,
    chargeback_with_transfer_id.transfer_id,
    chargeback_with_transfer_id.amount,
    chargeback_with_transfer_id.amount_in_usd,
    chargeback_with_transfer_id.flw_ref,
    chargeback_with_transfer_id.status,
    chargeback_with_transfer_id.updated_status,
    chargeback_with_transfer_id.updated_status_reason,
    chargeback_with_transfer_id.stage,
    chargeback_with_transfer_id.comment,
    chargeback_with_transfer_id.due_date,
    chargeback_with_transfer_id.due_date_45_days,
    chargeback_with_transfer_id.settlement_id,
    chargeback_with_transfer_id.created_at as chargeback_created_at,
    chargeback_with_transfer_id.transaction_id,
    chargeback_with_transfer_id.tx_ref,
    -- details about payment information
    payment_methods.hlo_table,
    payment_methods.external_provider,
    payment_methods.external_provider_transaction_id,
    payment_methods.transaction_details,
    payment_methods.payment_card_bin,
    payment_methods.payment_card_card_network,
    payment_methods.payment_card_issuing_bank,
    payment_methods.payment_card_card_type,
    payment_methods.payment_card_expiry_date,
    payment_methods.payment_card_last_four,
    payment_methods.nuban_bank_name,
    payment_methods.nuban_account_number,
    payment_methods.nuban_account_name,
    case when payment_methods.payment_card_bin is not null and payment_methods.nuban_bank_name is null then true else false end as is_card_chargeback,
    case when payment_methods.payment_card_bin is null and payment_methods.nuban_bank_name is not null then true else false end as is_bank_chargeback,
    -- more details on the deposit the chargeback was filed on
    expanded_ledgers.is_original_transfer_reversed,
    expanded_ledgers.hlo_status,
    expanded_ledgers.hlo_created_at as deposit_created_at,
    expanded_ledgers.hlo_updated_at as deposit_updated_at,
    -- auto decline stats
    case when audited_chargebacks.chargeback_id is null then false else true end as is_auto_declined,
    PARSE_JSON(audited_chargebacks.decline_api_response) as autodecline_response,
    audited_chargebacks.created_at as autodeclined_at
from {{ ref('mapping_chargebacks_to_chipper_transactions') }} as chargeback_with_transfer_id
left join
    {{ ref('expanded_ledgers') }} on
        expanded_ledgers.transfer_id = chargeback_with_transfer_id.transfer_id
left join
    {{ ref('deposits_all') }} as payment_methods on
        payment_methods.transfer_id = chargeback_with_transfer_id.transfer_id
left join 
    {{ ref('audited_chargebacks') }} as audited_chargebacks on
        chargeback_with_transfer_id.id = audited_chargebacks.chargeback_id
