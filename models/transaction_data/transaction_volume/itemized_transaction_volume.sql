{{ config(materialized='table') }}

{% set valid_transfer_types = (
        'AIRTIME_PURCHASES_COMPLETED',
        'ASSET_TRADES_BUY_SETTLED',
        'ASSET_TRADES_SELL_SETTLED',
        'BILL_PAYMENTS_COMPLETED',
        'CHECKOUTS_SETTLED',
        'CRYPTO_DEPOSITS_SETTLED',
        'CRYPTO_WITHDRAWALS_SETTLED',
        'DATA_PURCHASES_COMPLETED',
        'DEPOSITS_SETTLED',
        'ISSUED_CARD_TRANSACTIONS_FUNDING_COMPLETED',
        'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_COMPLETED',
        'NETWORK_API_B2C_SETTLED',
        'NETWORK_API_C2B_SETTLED',
        'PAYMENTS_CARD_DECLINED_FEE_FULL_SETTLED',
        'PAYMENTS_CARD_DECLINED_FEE_PARTIAL_SETTLED',
        'PAYMENTS_CARD_ISSUANCE_FEE_SETTLED',
        'PAYMENTS_CARD_WITHDRAWAL_FEE_SETTLED',
        'PAYMENTS_P2P_SETTLED',
        'PAYMENT_INVITATIONS_SETTLED',
        'REQUESTS_SETTLED',
        'S2NC_SETTLED',
        'STOCK_TRADES_BUY_SETTLED',
        'STOCK_TRADES_DIV_SETTLED',
        'STOCK_TRADES_DIVTAX_SETTLED',
        'STOCK_TRADES_MERGER_EXCHANGE_STOCK_CASH_SETTLED',
        'STOCK_TRADES_SELL_SETTLED',
        'WITHDRAWALS_SETTLED'
    )
%}

with transfers_with_external_id as (
    select 
        distinct transfer_id,
        external_provider_transaction_id
    from dbt_transformations.transaction_details
    where external_provider_transaction_id is not null
),

filtered_ledger as (
    select expanded_ledgers.*
    from dbt_transformations.expanded_ledgers
    left join transfers_with_external_id on expanded_ledgers.transfer_id = transfers_with_external_id.transfer_id
    where
        {# /* For CM-related volume (non-settled status but transaction processing attempted by provider, and hence have costs) */ #} 
        (expanded_ledgers.is_transfer_reversal = false and transfers_with_external_id.transfer_id is not null)

        {# /* For official processed volume calculations for investors, BOD, management, etc */ #}
        or (expanded_ledgers.transfer_type in {{valid_transfer_types}} and expanded_ledgers.is_original_transfer_reversed = false)
),

crypto_only_asset_trade_ledger_entries as (
    select filtered_ledger.*
    from filtered_ledger
    join {{ ref('assets') }} as assets on filtered_ledger.ledger_currency = assets.id
    where 
        journal_type = 'ASSET_TRADE'
        and assets.type = 'CRYPTO_CURRENCY'
),

filtered_ledger_without_asset_trades as (
    select filtered_ledger.*
    from filtered_ledger
    where journal_type != 'ASSET_TRADE'
),

deduped_filtered_ledger as (
    select * from crypto_only_asset_trade_ledger_entries
    union
    select * from filtered_ledger_without_asset_trades
),

ngn_parallel_rates as (
    select
        distinct date,
        rate,
        currency
    from utils.ngn_usd_parallel_market_rates
),

deduped_filtered_ledger_with_parallel as (
    select
        deduped_filtered_ledger.*,
        iff(ledger_currency = 'NGN', ngn_parallel_rates.rate, null) as ledger_parallel_rate,
        iff(
            ledger_currency = 'NGN',
            (ledger_amount / ledger_parallel_rate),
            ledger_amount_in_usd
        ) as ledger_amount_in_usd_parallel
    from deduped_filtered_ledger
    left join ngn_parallel_rates
        on cast(deduped_filtered_ledger.hlo_created_at as date) = ngn_parallel_rates.date
        and deduped_filtered_ledger.ledger_currency = ngn_parallel_rates.currency
),

transaction_corridor as (
    select
        distinct concat_ws('-', hlo_table, hlo_id) as hlo_table_with_id,
        origin_currency,
        destination_currency,
        concat_ws('-', origin_currency, destination_currency) as transaction_currency_pair,
        corridor
    from dbt_transformations.expanded_transfers
    where
        is_transfer_reversal = false
        and (
            (hlo_table = 'PAYMENT_INVITATIONS' and journal_type = 'INVITE_RESOLVE')
            or hlo_table != 'PAYMENT_INVITATIONS'
        )
),

chipper_processed_volume as (
    select
        'LEDGER_ENTRIES' as reference_table,
        ledger_entry_id,
        hlo_id,
        hlo_table,
        concat_ws('-', reference_table, ledger_entry_id::text) as reference_table_with_id,
        concat_ws('-', ledger.hlo_table, ledger.hlo_id) as hlo_table_with_id,
        
        transfer_id,
        transfer_type as transaction_type,
        journal_id,
        journal_type,
       
        true as is_chipper_processed_volume,
        is_original_transfer_reversed,
        is_transfer_reversal,
  
        activity_type as volume_type,

        origin_currency,
        destination_currency,
        transaction_currency_pair,
        transaction_corridor.corridor,
  
        hlo_status as transaction_status,
        hlo_updated_at as volume_timestamp,
        iff(ledger_amount < 0, 'DEBIT', 'CREDIT') as transaction_side,
  
        ledger_currency as volume_currency,
        ledger_rate as volume_rate_to_usd,
        ledger_parallel_rate as volume_parallel_rate,  
  
        abs(ledger_amount) as unadjusted_volume,
        abs(ledger_amount_in_usd) as unadjusted_volume_in_usd,
        abs(ledger_amount_in_usd_parallel) as unadjusted_volume_in_usd_parallel,
            
        iff((transfer_type in {{valid_transfer_types}} and is_original_transfer_reversed = false), unadjusted_volume, 0) as adjusted_volume,
        iff((transfer_type in {{valid_transfer_types}} and is_original_transfer_reversed = false), unadjusted_volume_in_usd, 0) 
            as adjusted_volume_in_usd,
        iff((transfer_type in {{valid_transfer_types}} and is_original_transfer_reversed = false), unadjusted_volume_in_usd_parallel, 0)
            as adjusted_volume_in_usd_parallel,
  
        main_party_user_id as transactor_id,
        primary_currency as transactor_primary_currency
    from deduped_filtered_ledger_with_parallel as ledger
    left join transaction_corridor
        on concat_ws('-', ledger.hlo_table, ledger.hlo_id) = transaction_corridor.hlo_table_with_id
    left join dbt_transformations.expanded_users on ledger.main_party_user_id = expanded_users.user_id
),

card_transactions_processed_by_providers as (
    select
        'ISSUED_CARD_TRANSACTIONS' as reference_table,
        ledger_entry_id,
        id::text as hlo_id,
        reference_table as hlo_table,
        concat_ws('-', reference_table, id::text) as reference_table_with_id,
        reference_table_with_id as hlo_table_with_id,
  
        issued_card_transactions_with_usd.transfer_id,

        iff(
            type = 'TRANSACTION',
            concat_ws('_', hlo_table, transaction_type),
            concat_ws('_', hlo_table, type, status)
        ) as transaction_type,

        journal_id,
        iff(type = 'TRANSACTION', 'CARD_SPEND', 'ISSUED_CARD') as journal_type,
  
        false as is_chipper_processed_volume,
        null as is_original_transfer_reversed,
        null as is_transfer_reversal,
  
        concat_ws('_', hlo_table, transaction_type) as volume_type,

        concat_ws('_', currency, 'CARD') as origin_currency,
        concat_ws('_', currency, 'CARD') as destination_currency,
        concat_ws('_', currency, 'CARD') as transaction_currency_pair,
        'DEBIT_CARD' as corridor,

        iff(type = 'TRANSACTION', base_ii_status, status) as transaction_status,
        convert_timezone('UTC', timestamp) as volume_timestamp,
        entry_type as transaction_side,
  
        currency as volume_currency,
        official_rate as volume_rate_to_usd,
        parallel_rate as volume_parallel_rate,
  
        abs(amount) as unadjusted_volume,
        abs(amount_in_usd) as unadjusted_volume_in_usd,
        abs(amount_in_usd_parallel) as unadjusted_volume_in_usd_parallel,
  
        iff(
            (type = 'TRANSACTION' and base_ii_status = 'C') 
                or (type != 'TRANSACTION' and status in ('SETTLED', 'COMPLETED') and reverse_transfer_id is null),
            unadjusted_volume,
            0
        ) as adjusted_volume,

        iff(
            (type = 'TRANSACTION' and base_ii_status = 'C') 
                or (type != 'TRANSACTION' and status in ('SETTLED', 'COMPLETED') and reverse_transfer_id is null),
            unadjusted_volume_in_usd,
            0
        ) as adjusted_volume_in_usd,

        iff(
            (type = 'TRANSACTION' and base_ii_status = 'C') 
                or (type != 'TRANSACTION' and status in ('SETTLED', 'COMPLETED') and reverse_transfer_id is null),
            unadjusted_volume_in_usd_parallel,
            0
        ) as adjusted_volume_in_usd_parallel,
  
        issued_card_transactions_with_usd.user_id as transactor_id,
        primary_currency as transactor_primary_currency
    from dbt_transformations_looker.issued_card_transactions_with_usd
    left join dbt_transformations.expanded_users on issued_card_transactions_with_usd.user_id = expanded_users.user_id
    left join (select transfer_id, ledger_entry_id from chipper_processed_volume) as chipper_volume
        on issued_card_transactions_with_usd.transfer_id = chipper_volume.transfer_id
    where 
        provider_transaction_id is not null
        and ledger_entry_id is null
)

select * from chipper_processed_volume
union
select * from card_transactions_processed_by_providers
