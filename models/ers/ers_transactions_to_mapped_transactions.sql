{{ config(materialized='ephemeral') }}

SELECT 
    ers.AMOUNT AS ers_amount,
    ers.DATA AS ers_data,
    ers.CREATED_AT AS ers_created_at,
    ers.EXTERNAL_ID AS ers_external_id,
    ers.TYPE AS ers_type,
    ers.UPDATED_AT AS ers_updated_at,
    ers.PROVIDER AS ers_provider,
    ers.RECORD_CREATION_DATE AS ers_record_creation_date,
    ers.CURRENCY AS ers_currency,
    ers.STATUS AS ers_status,
    ers.PROVIDER_FEE AS ers_provider_fee,
    ers.TRANSACTION_FEE AS ers_transaction_fee,
    ers.NET_AMOUNT AS ers_net_amount,
    ers.CATEGORY AS ers_category,
    CASE WHEN mapped_transactions.external_id IS NOT NULL THEN 'Y' ELSE 'N' END AS is_internal_record, 
    mapped_transactions.external_id,
    mapped_transactions.internal_id,
    mapped_transactions.ref_table_1,
    mapped_transactions.ref_table_2
FROM 
    "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
LEFT JOIN 
     {{ ref('mapped_transactions') }} as mapped_transactions
ON 
    ers.external_id = mapped_transactions.external_id
    