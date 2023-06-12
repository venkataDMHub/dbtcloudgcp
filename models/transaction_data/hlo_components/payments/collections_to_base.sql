{{ config(materialized='ephemeral') }}

with all_transfer_ids as (
    select
        payments.id as hlo_id,
        'PAYMENTS' as hlo_table,
        sender_id,
        recipient_id,
        payments.status as hlo_status,
        payments.transfer_id as original_transfer_id,
        payments.journal_id as original_transfer_journal_id,
        payments.created_at as hlo_created_at,
        payments.updated_at as hlo_updated_at,
        error_message,
        short_id,
        reference,
        note,
        payment_grouping,
        case
            when payment_grouping in ('CARD_DECLINED_FEE_FULL', 'CARD_DECLINED_FEE_PARTIAL', 'CARD_ISSUANCE_FEE', 'CARD_WITHDRAWAL_FEE') then concat(hlo_table, '_', payment_grouping, '_', payments.status)
            else concat(hlo_table, '_COLLECTIONS_TO_BASE_', payments.status) 
        end as original_transfer_payment_type,

        payments.reverse_transfer_id,
        reverse_transfers.journal_id as reverse_journal_id,
        case when payments.reverse_transfer_id is null then false
            else true
        end as is_original_transfer_reversed,
        'PAYMENTS_COLLECTIONS_TO_BASE_REVERSAL' as reverse_transfer_payment_type
    from
        "CHIPPER".{{ var("core_public") }}."PAYMENTS"
    left join
        "CHIPPER".{{ var("core_public") }}."TRANSFERS" as reverse_transfers
            on payments.reverse_transfer_id = reverse_transfers.id
    where
        sender_id not like 'base-%'
        and sender_id not like 'bot-%'
        and recipient_id like 'base-%'
        and payments.transfer_id is not null
),

transfer_ids as (
    select
        hlo_id,
        hlo_table,
        original_transfer_journal_id as hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        original_transfer_id as transfer_id,
        is_original_transfer_reversed,
        false as is_transfer_reversal,
        original_transfer_payment_type as payment_type
    from all_transfer_ids
),

reverse_transfer_ids as (
    select
        hlo_id,
        hlo_table,
        reverse_journal_id as hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        true as is_transfer_reversal,
        reverse_transfer_payment_type as payment_type
    from all_transfer_ids
    where is_original_transfer_reversed = true
)

select *
from transfer_ids

union

select *
from reverse_transfer_ids
