{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select data_purchases.id::text as  hlo_id,
        data_purchases.journal_id as hlo_journal_id,
        data_purchases.status as hlo_status,
        data_purchases.created_at as hlo_created_at,
        data_purchases.updated_at as hlo_updated_at,
        data_purchases.transfer_id as transfer_id,
        data_purchases.reverse_transfer_id as reverse_transfer_id,
        case
            when data_purchases.reverse_transfer_id is null then FALSE
            else TRUE
        end as is_original_transfer_reversed,
        data_purchases.data_provider as external_provider,
        data_purchases.external_id as external_provider_transaction_id,
        CARRIER,
        data_purchases.currency,
        phone_number,
        data_purchases.note as data_purchases_note,
        payments.note as payments_note,
        provider_response,
        case when data_purchases.reverse_transfer_id is null then data_purchases.user_id else null end as outgoing_user_id,
        case when data_purchases.reverse_transfer_id is not null then data_purchases.user_id else null end as incoming_user_id,

        iff(reversals_on_disbursements.transfer_id is null, false, true) as is_reverse_transfer_id_disbursement,
        iff(is_reverse_transfer_id_disbursement = false, payments.id::text, reversals_on_disbursements.id::text) as reverse_transfer_hlo_id,
        iff(is_reverse_transfer_id_disbursement = false, 'PAYMENTS', 'DISBURSEMENTS') as reverse_transfer_hlo_table,
        iff(is_reverse_transfer_id_disbursement = false, payments.journal_id, reversals_on_disbursements.journal_id) as reverse_transfer_journal_id,
        iff(is_reverse_transfer_id_disbursement = false, payments.status, reversals_on_disbursements.status) as reverse_transfer_hlo_status,
        iff(is_reverse_transfer_id_disbursement = false, payments.created_at, reversals_on_disbursements.created_at) as reverse_transfer_hlo_created_at,
        iff(is_reverse_transfer_id_disbursement = false, payments.updated_at, reversals_on_disbursements.updated_at) as reverse_transfer_hlo_updated_at

    from "CHIPPER".{{ var("core_public") }}."DATA_PURCHASES"
    left join "CHIPPER".{{ var("core_public") }}."PAYMENTS" as payments 
        on data_purchases.reverse_transfer_id = payments.transfer_id
    left join "CHIPPER".{{ var("core_public") }}."DISBURSEMENTS" as reversals_on_disbursements
        on data_purchases.reverse_transfer_id = reversals_on_disbursements.transfer_id
    where data_purchases.transfer_id is not null
    {% if is_incremental() %}
-- this filter will only be applied on an incremental run
and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
{% endif %}

),
transfer_ids as (
    select hlo_id,
        'DATA_PURCHASES' as hlo_table,
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
            when is_transfer_reversal = TRUE then 'DATA_PURCHASES_REVERSAL'
            else concat(hlo_table, '_', hlo_status) 
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'carrier', coalesce(CARRIER, try_parse_json('NULL')),
            'currency', coalesce(currency,try_parse_json('NULL')),
            'phoneNumber', coalesce(phone_number,try_parse_json('NULL')),
            'dataPurchaseNote', coalesce(data_purchases_note,try_parse_json('NULL')),
            'originalTransferForReverseTransferId',coalesce(REVERSE_TRANSFER_ID,try_parse_json('NULL'))                  
         ),
        'externalProviderTransactionDetails',provider_response
        ) as transaction_details,
        case when 
            transfer_type IN ('DATA_PURCHASES_COMPLETED') 
        THEN concat(
            'Bought data bundle for ',
            transaction_details:"_internalTransactionDetails":"carrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"phoneNumber"
        )

        WHEN transfer_type IN (
            'DATA_PURCHASES_NEW',
            'DATA_PURCHASES_PENDING',
            'DATA_PURCHASES_FAILED'
        )
        THEN concat(
            'Attempted data bundle purchase for ',
            transaction_details:"_internalTransactionDetails":"carrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"phoneNumber"
        )

        WHEN transfer_type IN ('DATA_PURCHASES_REVERSAL') 
        THEN transaction_details:"_internalTransactionDetails":"paymentNote"
        WHEN transfer_type in ('DATA_PURCHASES_QUEUED_FOR_REFUND')
        then transaction_details:"_internalTransactionDetails":"dataPurchaseNote"
        end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from all_transfer_ids
),
reverse_transfer_ids as (
    select reverse_transfer_hlo_id as hlo_id,
        reverse_transfer_hlo_table as hlo_table,
        reverse_transfer_journal_id as hlo_journal_id,
        reverse_transfer_hlo_status as hlo_status,
        reverse_transfer_hlo_created_at as hlo_created_at,
        reverse_transfer_hlo_updated_at as hlo_updated_at,
        all_transfer_ids.reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        TRUE as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when is_transfer_reversal = TRUE then 'DATA_PURCHASES_REVERSAL'
            else concat(hlo_table, '_', hlo_status) 
        end as transfer_type,
        object_construct(
            '_internalTransactionDetails',object_construct(
                'carrier', coalesce(CARRIER, try_parse_json('NULL')),
                'currency', coalesce(currency,try_parse_json('NULL')),
                'phoneNumber', coalesce(phone_number,try_parse_json('NULL')),
                'dataPurchaseNote', coalesce(data_purchases_note,try_parse_json('NULL')),
                'paymentNote',coalesce(payments_note,try_parse_json('NULL')),
                'reversalForOriginalTransferId',coalesce(all_transfer_ids.transfer_id,try_parse_json('NULL')),
                'reversalForOriginalJournalId',coalesce(hlo_journal_id,try_parse_json('NULL'))
            ),
            'externalProviderTransactionDetails',provider_response
        
        ) as transaction_details,
        case when 
            transfer_type IN ('DATA_PURCHASES_COMPLETED') 
        THEN concat(
            'Bought data bundle for ',
            transaction_details:"_internalTransactionDetails":"carrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"phoneNumber"
        )

        WHEN transfer_type IN (
            'DATA_PURCHASES_NEW',
            'DATA_PURCHASES_PENDING',
            'DATA_PURCHASES_FAILED'
        )
        THEN concat(
            'Attempted data bundle purchase for ',
            transaction_details:"_internalTransactionDetails":"carrier",
            ' ',
            transaction_details:"_internalTransactionDetails":"phoneNumber"
        )
        WHEN transfer_type IN ('DATA_PURCHASES_REVERSAL') 
        THEN transaction_details:"_internalTransactionDetails":"paymentNote"
        WHEN transfer_type in ('DATA_PURCHASES_QUEUED_FOR_REFUND')
        then transaction_details:"_internalTransactionDetails":"dataPurchaseNote"
        end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from all_transfer_ids
    where is_original_transfer_reversed = TRUE
        and reverse_transfer_hlo_id is not null
)
select *
from transfer_ids
union
select *
from reverse_transfer_ids
