{{  config(
        materialized='table',
        schema='intermediate') }}

select
    payments_with_type_with_ledger_entries.ledger_entry_id,
    payments_with_type_with_ledger_entries.transfer_id,
    payments_with_type_with_ledger_entries.is_original_transfer_reversed,
    payments_with_type_with_ledger_entries.is_transfer_reversal,
    payments_with_type_with_ledger_entries.transfer_type,
    payments_with_type_with_ledger_entries.hlo_journal_id as journal_id,
    journals.type as journal_type,
    payments_with_type_with_ledger_entries.hlo_id,
    payments_with_type_with_ledger_entries.hlo_table,
    payments_with_type_with_ledger_entries.hlo_status,
    transfers_with_usd.corridor,
    
    payments_with_type_with_ledger_entries.hlo_created_at,
    payments_with_type_with_ledger_entries.hlo_updated_at,
    payments_with_type_with_ledger_entries.currency as ledger_currency,
    payments_with_type_with_ledger_entries.amount as ledger_amount,

	case when ledger_amount < 0 then transfers_with_usd.origin_rate
    else transfers_with_usd.destination_rate end as ledger_rate,

	payments_with_type_with_ledger_entries.amount * ledger_rate as ledger_amount_in_usd,

    payments_with_type_with_ledger_entries.timestamp as ledger_timestamp,
    payments_with_type_with_ledger_entries.user_id as main_party_user_id, 

    case
        when user_to_user_payments_helper_table.hlo_journal_id is not null then
            coalesce(
                lag(payments_with_type_with_ledger_entries.user_id) over (
                    partition by payments_with_type_with_ledger_entries.hlo_journal_id 
                    order by payments_with_type_with_ledger_entries.amount
            ), 
                lead(payments_with_type_with_ledger_entries.user_id) over (
                    partition by payments_with_type_with_ledger_entries.hlo_journal_id 
                    order by payments_with_type_with_ledger_entries.amount
                )
            )
        
        else null
    end as counter_party_user_id

from 
    {{ ref('payments_with_type_with_ledger_entries') }} as payments_with_type_with_ledger_entries
join 
    "CHIPPER".{{ var("core_public") }}."JOURNALS"
        on journals.id = payments_with_type_with_ledger_entries.hlo_journal_id

join 
    {{ ref('transfers_with_usd')}}
        on payments_with_type_with_ledger_entries.transfer_id = transfers_with_usd.id

left join
    {{ ref('user_to_user_payments')}} as user_to_user_payments_helper_table
        on payments_with_type_with_ledger_entries.hlo_journal_id = user_to_user_payments_helper_table.hlo_journal_id
