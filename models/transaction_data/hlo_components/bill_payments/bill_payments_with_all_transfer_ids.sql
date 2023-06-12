{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select
        bill_payments.id::text as hlo_id,
        'BILL_PAYMENTS' as hlo_table,
        bill_payments.journal_id as hlo_journal_id,
        bill_payments.status as hlo_status,
        bill_payments.created_at as hlo_created_at,
        bill_payments.updated_at as hlo_updated_at,
        bill_payments.transfer_id as transfer_id,
        bill_payments.reverse_transfer_id as reverse_transfer_id,
        case when bill_payments.reverse_transfer_id is null
                  then FALSE
            else TRUE
        end as is_original_transfer_reversed,
        BILLER_NAME,
        BILLER_ITEM_ID,
        BILLER_ITEM_NAME,
        BILLER_ID,
        ENQUIRY_RESPONSE,
        PROVIDER as external_provider,
        EXTERNAL_ID as external_provider_transaction_id,
        case when bill_payments.reverse_transfer_id is null then bill_payments.user_id else null end as outgoing_user_id,
        case when bill_payments.reverse_transfer_id is not null then bill_payments.user_id else null end as incoming_user_id,

        iff(reversals_on_disbursements.transfer_id is null, false, true) as is_reverse_transfer_id_disbursement,
        iff(is_reverse_transfer_id_disbursement = false, bill_payments.id::text, reversals_on_disbursements.id::text) as reverse_transfer_hlo_id,
        iff(is_reverse_transfer_id_disbursement = false, 'BILL_PAYMENTS', 'DISBURSEMENTS') as reverse_transfer_hlo_table,
        iff(is_reverse_transfer_id_disbursement = false, bill_payments.journal_id, reversals_on_disbursements.journal_id) as reverse_transfer_journal_id,
        iff(is_reverse_transfer_id_disbursement = false, bill_payments.status, reversals_on_disbursements.status) as reverse_transfer_hlo_status,
        iff(is_reverse_transfer_id_disbursement = false, bill_payments.created_at, reversals_on_disbursements.created_at) as reverse_transfer_hlo_created_at,
        iff(is_reverse_transfer_id_disbursement = false, bill_payments.updated_at, reversals_on_disbursements.updated_at) as reverse_transfer_hlo_updated_at

    from "CHIPPER".{{var("core_public")}}."BILL_PAYMENTS"
    left join "CHIPPER".{{ var("core_public") }}."DISBURSEMENTS" as reversals_on_disbursements
        on (
            bill_payments.reverse_transfer_id = reversals_on_disbursements.transfer_id
            and bill_payments.id = reversals_on_disbursements.product_id
        )
    where bill_payments.transfer_id is not null
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    
{% endif %}),

transfer_ids as (
    select
        hlo_id,
        hlo_table,
        hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        transfer_id,
        is_original_transfer_reversed,
        FALSE as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when is_transfer_reversal = TRUE then 'BILL_PAYMENTS_REVERSAL'
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'billerName', coalesce(BILLER_NAME, try_parse_json('NULL')),
            'billerItemId', coalesce(BILLER_ITEM_ID,try_parse_json('NULL')),
            'billerItemName', coalesce(BILLER_ITEM_NAME,try_parse_json('NULL')),
            'billerId', coalesce(BILLER_ID,try_parse_json('NULL')),
            'originalTransferForReverseTransferId',coalesce(REVERSE_TRANSFER_ID,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',ENQUIRY_RESPONSE
        ) as transaction_details,
        'bill_payments' as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id


    from all_transfer_ids
),

reverse_transfer_ids as (
    select
        reverse_transfer_hlo_id as hlo_id,
        reverse_transfer_hlo_table as hlo_table,
        reverse_transfer_journal_id as hlo_journal_id,
        reverse_transfer_hlo_status as hlo_status,
        reverse_transfer_hlo_created_at as hlo_created_at,
        reverse_transfer_hlo_updated_at as hlo_updated_at,
        reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        TRUE as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when is_transfer_reversal = TRUE then 'BILL_PAYMENTS_REVERSAL'
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'billerName', coalesce(BILLER_NAME, try_parse_json('NULL')),
            'billerItemId', coalesce(BILLER_ITEM_ID,try_parse_json('NULL')),
            'billerItemName', coalesce(BILLER_ITEM_NAME,try_parse_json('NULL')),
            'billerId', coalesce(BILLER_ID,try_parse_json('NULL')),
            'reversalForOriginalTransferId',coalesce(transfer_id,try_parse_json('NULL')),
            'reversalForOriginalJournalId',coalesce(hlo_journal_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',ENQUIRY_RESPONSE
        ) as transaction_details,
        'bill_payments' as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id


    from all_transfer_ids
    where is_original_transfer_reversed = TRUE
)

select * from transfer_ids
union
select * from reverse_transfer_ids
