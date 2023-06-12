{{ config(materialized='view') }}

/*
The below CTE attempts to get the transfer_id for the corresponding flutterwave reference (FLW_REF)
This is done by parsing the transaction_details JSON. Not all the JSON's have the same structure
*/
with flutterwave_to_transaction as (
    (
        select
            transfer_id,
            transaction_details:externalProviderTransactionDetails:processor_response:data:flwRef::text as flw_ref
        from {{ ref('transaction_details') }}
        where flw_ref is not null
    )
    union
    (
        select
            transfer_id,
            transaction_details:externalProviderTransactionDetails:flwRef::text as flw_ref
        from {{ ref('transaction_details') }}
        where flw_ref is not null
    )
    union
    (
        select
            transfer_id,
            transaction_details:externalProviderTransactionDetails:status_settled:data:flwref::text as flw_ref
        from {{ ref('transaction_details') }}
        where flw_ref is not null
    )
)

select
    transfer_id,
    flw_ref
from
    flutterwave_to_transaction
