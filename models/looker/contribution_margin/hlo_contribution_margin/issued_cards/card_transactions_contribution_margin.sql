{{ config(materialized='table', schema='intermediate', unique_key='hlo_table_with_id') }}

with
    parallel_rates as (

        select distinct date, rate, currency
        from chipper.utils.ngn_usd_parallel_market_rates

    ),

    all_card_transactions as (

        select card_trans.*,
        row_number() over (partition by card_trans.provider_transaction_id order by card_trans.created_at desc) as row_num
        from {{ ref('issued_card_transactions') }} as card_trans

    ),

    card_transactions_without_duplicates as (

        select card_trans.*
        from all_card_transactions as card_trans
        where card_trans.provider_transaction_id IS NULL
        OR card_trans.row_num = 1

    ),

    issued_cards as (

        select
            'ISSUED_CARD' as margin_stream,
            'CARD_SPEND_FEES_ESTIMATED' as revenue_stream,

            date_trunc('month', timestamp) as month,

            card_transactions.updated_at as transaction_settled_date,

            cards.card_issuer as external_provider,
            cards.currency as card_currency,
            card_transactions.card_id,
            card_transactions.id::text as card_transaction_id,
            card_transactions.provider_transaction_id,
            card_transactions.country,
            card_transactions.fee_currency,
            card_transactions.description as transaction_description,
            card_transactions.created_at,
            card_transactions.type as transaction_type,
            card_transactions.provider_details,
            card_transactions.provider_card_id,
            card_transactions.timestamp,
            card_transactions.card_last_four,
            card_transactions.currency,
            card_transactions.journal_id,
            card_transactions.PROVIDER_DETAILS:BaseIIStatus::text as base_II_status,
            card_transactions.error_message,
            card_transactions.transfer_id,
            card_transactions.reverse_transfer_id,
            card_transactions.user_id,
            card_transactions.entry_type as debit_or_credit,
            card_transactions.status as hlo_status,
            card_transactions.amount as card_transaction_amount,
            card_transactions.fee_amount as card_fee_amount,

            users.primary_currency,
            users.kyc_tier as user_kyc_tier,
            users.acquisition_date as user_acquisition_date,
            users.acquisition_source as user_acquisition_source,
            users.is_internal as user_is_internal,
            users.is_deleted as user_is_deleted,
            users.is_admin as user_is_admin,
            users.is_business as user_is_business,
            users.is_valid_user as user_is_valid_user,
            users.is_blocked_by_flag as user_is_blocked_by_flag,
            users.has_risk_flag as user_has_risk_flag,

            ngn_parallel_rates.rate as parallel_rate,

            whales.is_whale,
            whales.is_parallel_whale,

            revenues.rate_to_usd as rate,

            transfers.transfer_type,
            transfers.journal_type,
            transfers.corridor,

            volume.hlo_table_with_id,
            volume.unadjusted_transaction_volume_in_usd as tpv_in_usd,
            volume.unadjusted_transaction_volume_in_usd_parallel as tpv_in_usd_parallel,

            revenues.net_revenues,
            revenues.net_revenues_in_usd,
            revenues.net_revenues_in_usd_parallel,

            -- row number for each individual card, 0.25 fee for each card signup
            row_number() over (partition by card_transactions.card_id order by timestamp) as card_signup_row_number,
            case when card_signup_row_number = '1' then 0.25 else 0 end as card_signup_fee,

            -- row number for each individual card per month, 0.10 fee for each monthly active user
            row_number() over (partition by month, card_transactions.card_id order by timestamp) as active_user_row_number,
            case when active_user_row_number = '1' then 0.10 else 0 end as active_user_fee,

            -- 0.45 fee for each declined transaction
            case when status = 'FAILED' then 0.45 else 0 end as decline_fee,

            -- 0.035 fee for each card transaction
            0.035 as transaction_fee,
            
            (card_signup_fee + active_user_fee + transaction_fee + decline_fee) as cogs_in_usd,
            cogs_in_usd as cogs_in_usd_parallel,

            coalesce(net_revenues_in_usd,0) - coalesce(cogs_in_usd,0) as gross_margin_in_usd,
            coalesce(net_revenues_in_usd_parallel,0) - coalesce(cogs_in_usd,0) as gross_margin_in_usd_parallel,
            
            gross_margin_in_usd as contribution_margin_in_usd,
            gross_margin_in_usd_parallel as contribution_margin_in_usd_parallel

        from 
            card_transactions_without_duplicates as card_transactions

        inner join
            {{ ref('staging_issued_cards') }} as cards
            on card_transactions.card_id = cards.id

        left join
            chipper.dbt_transformations.user_demographic_features as users
            on card_transactions.user_id = users.user_id
        
        left join
            chipper.dbt_transformations_looker.revenue_details as revenues
            on card_transactions.provider_transaction_id = revenues.external_provider_transaction_id
            and revenues.revenue_stream = 'CARD_SPEND_FEES_ESTIMATED' 

        left join
            chipper.dbt_transformations.expanded_transfers as transfers
            on card_transactions.transfer_id = transfers.transfer_id

        left join
            chipper.dbt_transformations.aggregated_transaction_volume as volume
            on concat_ws('-', 'ISSUED_CARD_TRANSACTIONS', card_transactions.id::text) = volume.hlo_table_with_id
        
        left join
            parallel_rates as ngn_parallel_rates
            on cast(card_transactions.timestamp as date) = ngn_parallel_rates.date
            and card_transactions.currency = ngn_parallel_rates.currency

        left join  
            chipper.dbt_transformations_looker.whales_monthly as whales
            on whales.monetized_user_id = card_transactions.user_id
            and whales.settled_month = date_trunc('month', timestamp)
            and whales.revenue_stream = margin_stream

        where 
            cards.card_issuer = 'GTP'
            and card_transactions.provider_transaction_id is not null
            and users.user_id not in ({{internal_users()}})
            and volume.hlo_table_with_id is not null
    )

    select 
    margin_stream,
    revenue_stream,
    transaction_settled_date,
    card_id,
    card_transaction_id,
    provider_transaction_id,
    country,
    card_currency,
    fee_currency,
    transaction_description,
    transaction_type,
    external_provider,
    provider_details,
    provider_card_id,
    journal_id,
    base_II_status,
    error_message,
    transfer_id,
    corridor,
    reverse_transfer_id,
    user_id,
    debit_or_credit,
    hlo_status,
    card_transaction_amount,
    card_fee_amount,
    primary_currency,
    user_kyc_tier,
    user_acquisition_date,
    user_acquisition_source,
    user_is_internal,
    user_is_deleted,
    user_is_admin,
    user_is_business,
    user_is_valid_user,
    user_is_blocked_by_flag,
    user_has_risk_flag,
    is_whale,
    is_parallel_whale,
    transfer_type,
    journal_type,
    rate,
    parallel_rate,
    cast(card_signup_fee as float) as card_signup_fee,
    cast(active_user_fee as float) as active_user_fee,
    cast(decline_fee as float) as decline_fee,
    cast(transaction_fee as float) as transaction_fee,
    hlo_table_with_id,
    tpv_in_usd,
    tpv_in_usd_parallel,
    net_revenues_in_usd,
    net_revenues_in_usd_parallel,
    cogs_in_usd,
    cogs_in_usd_parallel,
    gross_margin_in_usd,
    gross_margin_in_usd_parallel,
    0 as operating_expenses_in_usd,
    0 as operating_expenses_in_usd_parallel,
    contribution_margin_in_usd,
    contribution_margin_in_usd_parallel
    from issued_cards
