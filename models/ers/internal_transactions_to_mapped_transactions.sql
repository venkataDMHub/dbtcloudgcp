{{ config(materialized='ephemeral') }}

SELECT 
    transactions.TRANSFER_ID as internal_txn_id,
    transactions.EXTERNAL_PROVIDER as internal_txn_external_provider,
    transactions.EXTERNAL_PROVIDER_TRANSACTION_ID as internal_txn_external_provider_id,
    transactions.TRANSACTION_DETAILS as internal_txn_details,
    transactions.HLO_CREATED_AT as internal_txn_created_at,
    transactions.HLO_UPDATED_AT as internal_txn_updated_at,
    transactions.HLO_STATUS as internal_txn_status,
    ledgers.TRANSFER_TYPE as internal_txn_transfer_type,
    ledgers.JOURNAL_TYPE as internal_txn_journal_type,
    ledgers.JOURNAL_ID as internal_txn_journal_id,
    ledgers.CORRIDOR as internal_txn_corridor,
    ledgers.LEDGER_CURRENCY as internal_txn_ledger_currency,
    ledgers.LEDGER_AMOUNT as internal_txn_ledger_amount,
    ledgers.LEDGER_RATE as internal_txn_ledger_rate,
    ledgers.LEDGER_AMOUNT_IN_USD as internal_txn_ledger_amount_in_usd,
    CASE WHEN mapped_transactions.external_id IS NOT NULL THEN 'Y' ELSE 'N' END AS is_ers_record, 
    mapped_transactions.external_id,
    mapped_transactions.internal_id,
    mapped_transactions.ref_table_1,
    mapped_transactions.ref_table_2
FROM 
    "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
LEFT JOIN 
    "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_LEDGERS" as ledgers
ON
    transactions.transfer_id = ledgers.transfer_id
LEFT JOIN 
    {{ ref('mapped_transactions') }} as mapped_transactions
ON 
    transactions.transfer_id = mapped_transactions.internal_id
