{{ config(materialized='table',
          schema='looker') }}

with ledger_details as (

    select
        ledgers.*,
        abs(ledgers.ledger_amount_in_usd) as absolute_ledger_amount_in_usd,

        abs(CASE 
            WHEN ledgers.ledger_currency != 'NGN' THEN ledgers.ledger_amount_in_usd
            WHEN ledgers.ledger_currency = 'NGN' AND  ngn_parallel.rate IS NOT NULL THEN ledgers.ledger_amount / ngn_parallel.rate
            ELSE NULL
        END) absolute_ledger_amount_in_usd_parallel,  
        abs(ledgers.ledger_amount) as absolute_ledger_amount,

        volume.adjusted_volume as local_tpv,
        volume.adjusted_volume_in_usd as usd_tpv,
        volume.adjusted_volume_in_usd_parallel as usd_tpv_parallel

    from dbt_transformations.expanded_ledgers as ledgers
    left join dbt_transformations.itemized_transaction_volume as volume 
        on ledgers.ledger_entry_id = volume.ledger_entry_id
        and volume.ledger_entry_id is not null
    LEFT JOIN chipper.utils.ngn_usd_parallel_market_rates AS ngn_parallel
        ON ledgers.ledger_currency = ngn_parallel.currency
        AND cast(ledgers.ledger_timestamp as DATE) = cast(ngn_parallel.date as DATE)
)

select
    ledger_entry_id,
    transfer_id,
    main_party_user_id,
    counter_party_user_id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    transfer_type,
    activity_type,
    journal_id,
    journal_type,
    hlo_id,
    hlo_table,
    hlo_status,
    corridor,
    hlo_created_at,
    hlo_updated_at,
    ledger_timestamp,
    ledger_currency,
    ledger_amount,
    absolute_ledger_amount,
    ledger_rate,
    ledger_amount_in_usd,
    absolute_ledger_amount_in_usd,
    local_tpv,
    usd_tpv, 
    usd_tpv_parallel
from ledger_details
