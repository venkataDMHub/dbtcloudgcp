{{ config(materialized='table') }}

with recon_master_set as (

SELECT
    ers_amount,
    ers_data,
    ers_created_at,
    ers_external_id,
    ers_type,
    ers_updated_at,
    ers_provider,
    ers_record_creation_date,
    ers_currency,
    ers_status,
    ers_provider_fee,
    ers_transaction_fee,
    ers_net_amount,
    ers_category,
    is_internal_record, 
    internal_txn_id,
    internal_txn_external_provider,
    internal_txn_external_provider_id,
    internal_txn_details,
    internal_txn_created_at,
    internal_txn_updated_at,
    internal_txn_status,
    internal_txn_transfer_type,
    internal_txn_journal_type,
    internal_txn_journal_id,
    internal_txn_corridor,
    internal_txn_ledger_currency,
    internal_txn_ledger_amount,
    internal_txn_ledger_rate,
    internal_txn_ledger_amount_in_usd,
    is_ers_record, 
    case when chargebacks.transfer_id is not null then 'Y' else 'N' end as is_chargeback,
    chargebacks.status as chargeback_status,
    chargebacks.updated_status as chargeback_updated_status, 
    chargebacks.amount as chargeback_claim_amount, 
    chargebacks.amount_in_usd as chargeback_claim_amount_in_usd,
    coalesce(ers_transactions.external_id, internal_transactions.external_id) as external_id,
    coalesce(ers_transactions.internal_id, internal_transactions.internal_id) as internal_id,
    coalesce(ers_transactions.ref_table_1, internal_transactions.ref_table_1) as external_reference_table,
    coalesce(ers_transactions.ref_table_2, internal_transactions.ref_table_2) as internal_reference_table
FROM {{ ref('ers_transactions_to_mapped_transactions') }} as ers_transactions
FULL OUTER JOIN {{ ref('internal_transactions_to_mapped_transactions') }} as internal_transactions
ON ers_transactions.external_id = internal_transactions.external_id
AND ers_transactions.internal_id = internal_transactions.internal_id
LEFT JOIN {{ ref('chargebacks_for_ers') }} as chargebacks
ON internal_transactions.internal_txn_id = chargebacks.transfer_id

)

select * from recon_master_set
