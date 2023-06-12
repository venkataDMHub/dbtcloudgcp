{{ config(materialized='table', schema='looker') }}


SELECT 
    journal_id::text AS journal_id, 
    main_party_user_id AS user_id, 
    journal_type, 
    transfer_type,
    iff(main_party_users.primary_currency != counter_party_users.primary_currency, 'CROSS_BORDER', 'LOCAL') as nationality_corridor,
    ledger_timestamp, 
    hlo_status, 
    is_original_transfer_reversed
FROM {{ref('expanded_ledgers')}}
    LEFT JOIN dbt_transformations.expanded_users as main_party_users 
        ON expanded_ledgers.main_party_user_id = main_party_users.user_id
    LEFT JOIN dbt_transformations.expanded_users as counter_party_users 
        ON expanded_ledgers.counter_party_user_id = counter_party_users.user_id 

UNION

SELECT
    concat_ws('_', issued_cards.card_issuer, card_trans.provider_transaction_id) AS journal_id,
    issued_cards.user_id, 
    'CARD_SPEND' AS journal_type, 
    'CARD_SPEND' AS transfer_type, 
    null as nationality_corridor,
    convert_timezone('UTC', card_trans.timestamp) AS ledger_timestamp, 
    status AS hlo_status, 
    CASE WHEN reverse_transfer_id IS null THEN false ELSE true END AS is_original_transfer_reversed
FROM {{ref('issued_card_transactions_with_usd')}} as card_trans
    LEFT JOIN {{var("core_public")}}.issued_cards ON (
        card_trans.card_id = issued_cards.id AND
        card_trans.provider_card_id = issued_cards.provider_card_id)
WHERE card_trans.type = 'TRANSACTION'
