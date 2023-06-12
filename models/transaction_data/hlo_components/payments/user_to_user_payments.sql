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
            {# /* All Network API B2C transactions have a payment grouping.
                The Finance and LRC teams also requested that the type be named
                Network API B2C (without the HLO table prefix). */ #}
            when payment_grouping = 'NETWORK_API_PAYOUT' then concat('NETWORK_API_B2C_', payments.status)

            {# /* Not all Account Merges have a payment grouping, 
                but the "Combining balances" string has always been used since the beginning.
                More about this in ChipperCore/app/controllers/admin/index.ts (combineUsers function). */ #}
            when payment_grouping = 'ACCOUNT_MERGES' then concat(hlo_table, '_ACCOUNT_MERGES_', payments.status)
            when payment_grouping = 'PENDING_FUNDS_REVERSAL' then concat(hlo_table, '_PENDING_FUNDS_REVERSAL_', payments.status)
            when lower(note) like '%combining balances%' then concat(hlo_table, '_ACCOUNT_MERGES_', payments.status)

            {# /* There are only 3 types of user-to-user payments:
                1. Network API B2C
                2. Account Merges
                3. If neither 1 nor 2, then it's a P2P payment */ #}
            else concat(hlo_table, '_P2P_', payments.status)
        end as original_transfer_payment_type,

        payments.reverse_transfer_id,
        reverse_transfers.journal_id as reverse_journal_id,
        case when payments.reverse_transfer_id is null then false
            else true
        end as is_original_transfer_reversed,
        'PAYMENTS_USER_TO_USER_REVERSAL' as reverse_transfer_payment_type
    from
        "CHIPPER".{{ var("core_public") }}."PAYMENTS"
    left join "CHIPPER".{{ var("core_public") }}."TRANSFERS" as reverse_transfers
        on payments.reverse_transfer_id = reverse_transfers.id
    where
        sender_id not like 'base-%'
        and sender_id not like 'bot-%'
        and recipient_id not like 'base-%'
        and recipient_id not like 'bot-%'
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
