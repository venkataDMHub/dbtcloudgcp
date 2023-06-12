{{ config(materialized='ephemeral') }}

with external_provider_transaction_id_join as (

SELECT 
    ers.external_id as external_id, 
    transactions.transfer_id as internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'TRANSACTION_DETAILS' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
on ers.DATA:id::text = transactions.EXTERNAL_PROVIDER_TRANSACTION_ID
  
),
  
txid_id_join as (

SELECT 
    ers.external_id as external_id, 
    transactions.transfer_id as internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'TRANSACTION_DETAILS' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
on ers.DATA:id::text = transactions.TRANSACTION_DETAILS:externalProviderTransactionDetails:status_settled:data:txid::text

),

internal_json_id_join as (

SELECT 
    ers.external_id as external_id, 
    transactions.transfer_id as internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'TRANSACTION_DETAILS' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
on ers.DATA:id::text = transactions.transaction_details:externalProviderTransactionDetails:processor_response:data:id::text

),

status_settled_json_id_join as (

SELECT 
    ers.external_id as external_id, 
    transactions.transfer_id as internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'TRANSACTION_DETAILS' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
on ers.DATA:id::text = transactions.transaction_details:externalProviderTransactionDetails:status_settled:id::text

),

externalprovider_id_join as (

SELECT 
    ers.external_id as external_id, 
    transactions.transfer_id as internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'TRANSACTION_DETAILS' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
on ers.DATA:id::text = transactions.transaction_details:externalProviderTransactionDetails:id::text

),

flwref_join as (

SELECT 
    ers.external_id as external_id, 
    transactions.transfer_id as internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'TRANSACTION_DETAILS' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
on ers.DATA:flw_ref::text = transactions.transaction_details:externalProviderTransactionDetails:flwRef::text

),

card_verification_charges_join as (

select 
    ers.external_id as external_id, 
    TRY_TO_NUMBER(CONCAT('CVC-',card_verification_charges.ID)) AS internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'CARD_VERIFICATION_CHARGES' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER".{{var("core_public")}}."CARD_VERIFICATION_CHARGES" as card_verification_charges
on ers.DATA:tx_ref::text = 
COALESCE(PROCESSOR_RESPONSE:error:options:body:txRef::text,
PROCESSOR_RESPONSE:error:options:body:reference::text,
PROCESSOR_RESPONSE:data:txRef::text,
PROCESSOR_RESPONSE:data:reference::text)
  

),

json_reference_join as (

select 
    ers.external_id as external_id, 
    transactions.transfer_id as internal_id, 
    'EXTERNAL_RECONCILIATION_RECORDS' AS ref_table_1, 
    'TRANSACTION_DETAILS' AS ref_table_2
from "CHIPPER".{{var("core_public")}}."EXTERNAL_RECONCILIATION_RECORDS" as ers
join "CHIPPER"."DBT_TRANSFORMATIONS"."TRANSACTION_DETAILS" as transactions
on ers.DATA:reference::text = transactions.transaction_details:externalProviderTransactionDetails:settled:transfer:reference::text


)

SELECT DISTINCT 
    external_id, 
    internal_id, 
    ref_table_1, 
    ref_table_2 

FROM (

SELECT * FROM external_provider_transaction_id_join
UNION 
SELECT * FROM txid_id_join
UNION 
SELECT * FROM internal_json_id_join
UNION 
SELECT * FROM status_settled_json_id_join
UNION 
SELECT * FROM externalprovider_id_join
UNION 
SELECT * FROM flwref_join
UNION 
SELECT * FROM card_verification_charges_join
UNION
SELECT * FROM json_reference_join

)
