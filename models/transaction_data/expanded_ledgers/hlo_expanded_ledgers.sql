{{ config(
        materialized='incremental',
        unique_key='ledger_entry_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

{% set p2p_settled_transfer_types = (
        'PAYMENTS_P2P_SETTLED',
        'PAYMENT_INVITATIONS_SETTLED',
        'REQUESTS_SETTLED'
    )
%}

{% set chipper_rewards_payout_settled_transfer_types = (
        'CASHBACKS_SETTLED',
        'PAYMENTS_ACTIVATION_TOOLING_PAYOUT_SETTLED',
        'PAYMENTS_CASHBACK_SETTLED',
        'PAYMENTS_MOBILE_MONEY_CONSUMER_FEE_REIMBURSEMENT_SETTLED',
        'PAYMENTS_REFERRAL_BONUS_SETTLED',
        'PAYMENTS_WELCOME_BONUS_SETTLED',
        'STOCK_TRADES_REWARD_SETTLED',
        'REFERRAL_BONUS_SETTLED'
    )
%}

with all_hlo_expanded_ledgers as (
    {{ dbt_utils.union_relations(
        relations=[ref('airtime_purchases_expanded_ledgers'), 
                   ref('asset_trades_expanded_ledgers'), 
                   ref('bill_payments_expanded_ledgers'),
                   ref('cashback_disbursements_expanded_ledgers'),
                   ref('checkouts_expanded_ledgers'),
                   ref('crypto_deposits_expanded_ledgers'),
                   ref('crypto_withdrawals_expanded_ledgers'),
                   ref('data_purchases_expanded_ledgers'),
                   ref('deposits_expanded_ledgers'),
                   ref('issued_card_transactions_expanded_ledgers'),
                   ref('orders_expanded_ledgers'),
                   ref('payment_invitations_expanded_ledgers'),
                   ref('payments_expanded_ledgers'),
                   ref('referral_bonus_disbursements_expanded_ledgers'),
                   ref('requests_expanded_ledgers'),
                   ref('stock_trades_expanded_ledgers'),
                   ref('withdrawals_expanded_ledgers')
                   ]
    ) }}
)

SELECT 
    ledger_entry_id,
    transfer_id,
    is_original_transfer_reversed,
    is_transfer_reversal,
    transfer_type,
    journal_id,
    journal_type,
    hlo_id,
    hlo_table,
    hlo_status,
    corridor,
    hlo_created_at,
    hlo_updated_at,
    ledger_currency,
    ledger_amount,
    ledger_rate,
    ledger_amount_in_usd,
    ledger_timestamp,
    main_party_user_id,
    counter_party_user_id,

    case
        when transfer_type = 'NETWORK_API_B2C_SETTLED' and ledger_amount < 0 then 'NETWORK_API_B2C_SENT'
        when transfer_type = 'NETWORK_API_B2C_SETTLED' and ledger_amount > 0 then 'NETWORK_API_B2C_RECEIVED'

        when transfer_type = 'NETWORK_API_C2B_SETTLED' and ledger_amount < 0 then 'NETWORK_API_C2B_SENT'
        when transfer_type = 'NETWORK_API_C2B_SETTLED' and ledger_amount > 0 then 'NETWORK_API_C2B_RECEIVED'

        when transfer_type in {{p2p_settled_transfer_types}} and ledger_amount < 0 and corridor = 'CROSS_BORDER_FIAT'
            then 'P2P_SENT_CROSS_BORDER'

        when transfer_type in {{p2p_settled_transfer_types}} and ledger_amount < 0 and corridor = 'LOCAL_FIAT'
            then 'P2P_SENT_LOCAL'

        when transfer_type in {{p2p_settled_transfer_types}} and ledger_amount > 0 and corridor = 'CROSS_BORDER_FIAT'
            then 'P2P_RECEIVED_CROSS_BORDER'

        when transfer_type in {{p2p_settled_transfer_types}} and ledger_amount > 0 and corridor = 'LOCAL_FIAT'
            then 'P2P_RECEIVED_LOCAL'

        when transfer_type in {{chipper_rewards_payout_settled_transfer_types}} then 'CHIPPER_REWARDS_PAYOUT'

        when transfer_type = 'S2NC_SETTLED' and corridor = 'CROSS_BORDER_FIAT' then 'S2NC_CROSS_BORDER'
        when transfer_type = 'S2NC_SETTLED' and corridor = 'LOCAL_FIAT' then 'S2NC_LOCAL'

        when transfer_type like '%_SETTLED' then regexp_replace(transfer_type, '_SETTLED', '')
        when transfer_type like '%_COMPLETED' then regexp_replace(transfer_type, '_COMPLETED', '')

        else transfer_type
    end as activity_type
FROM 
    all_hlo_expanded_ledgers
