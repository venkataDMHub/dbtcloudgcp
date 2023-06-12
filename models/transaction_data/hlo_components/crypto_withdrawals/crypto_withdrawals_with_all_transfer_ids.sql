{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}



with all_transfer_ids as (
    select
        crypto_withdrawals.id::text as hlo_id,
        'CRYPTO_WITHDRAWALS' as hlo_table,
        crypto_withdrawals.journal_id as hlo_journal_id,
        crypto_withdrawals.status as hlo_status,
        crypto_withdrawals.created_at as hlo_created_at,
        crypto_withdrawals.updated_at as hlo_updated_at,
        crypto_withdrawals.transfer_id as transfer_id,
        crypto_withdrawals.reverse_transfer_id as reverse_transfer_id,
        case when crypto_withdrawals.reverse_transfer_id is null then false
            else true
        end as is_original_transfer_reversed,
        PROVIDER as external_provider,
        PROVIDER_TRANSACTION_ID as external_provider_transaction_id,
        asset,
        address,
        transaction_hash,
        fee,
        PROVIDER_DETAILS,
        case when crypto_withdrawals.reverse_transfer_id is null then crypto_withdrawals.user_id else null end as outgoing_user_id,
        case when crypto_withdrawals.reverse_transfer_id is not null then crypto_withdrawals.user_id else null end as incoming_user_id,

        iff(reversals_on_disbursements.transfer_id is null, false, true) as is_reverse_transfer_id_disbursement,
        iff(is_reverse_transfer_id_disbursement = false, crypto_withdrawals.id::text, reversals_on_disbursements.id::text) as reverse_transfer_hlo_id,
        iff(is_reverse_transfer_id_disbursement = false, 'CRYPTO_WITHDRAWALS', 'DISBURSEMENTS') as reverse_transfer_hlo_table,
        iff(is_reverse_transfer_id_disbursement = false, reverse_transfers.journal_id, reversals_on_disbursements.journal_id) as reverse_transfer_journal_id,
        iff(is_reverse_transfer_id_disbursement = false, crypto_withdrawals.status, reversals_on_disbursements.status) as reverse_transfer_hlo_status,
        iff(is_reverse_transfer_id_disbursement = false, crypto_withdrawals.created_at, reversals_on_disbursements.created_at) as reverse_transfer_hlo_created_at,
        iff(is_reverse_transfer_id_disbursement = false, crypto_withdrawals.updated_at, reversals_on_disbursements.updated_at) as reverse_transfer_hlo_updated_at

    from
        "CHIPPER".{{ var("core_public") }}."CRYPTO_WITHDRAWALS"
    left join "CHIPPER".{{ var("core_public") }}."TRANSFERS" as reverse_transfers
        on crypto_withdrawals.reverse_transfer_id = reverse_transfers.id
    left join "CHIPPER".{{ var("core_public") }}."DISBURSEMENTS" as reversals_on_disbursements
        on crypto_withdrawals.reverse_transfer_id = reversals_on_disbursements.transfer_id
    where crypto_withdrawals.transfer_id is not null
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}
),

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
        false as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when is_transfer_reversal = true then concat(hlo_table, '_', 'REVERSAL')
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'asset', coalesce(asset, try_parse_json('NULL')),
            'address', coalesce(address,try_parse_json('NULL')),
            'transactionHash', coalesce(transaction_hash,try_parse_json('NULL')),
            'fee',coalesce(fee::number(10,2),try_parse_json('NULL')),
            'originalTransferForReverseTransferId',coalesce(reverse_transfer_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',coalesce(PROVIDER_DETAILS,try_parse_json('NULL'))
        ) as transaction_details,
        case when 
        transfer_type IN ('CRYPTO_WITHDRAWALS_SETTLED') 
            AND external_provider = 'COINBASE' 
            THEN concat(
                'Withdrew ',
                transaction_details:"_internalTransactionDetails":"asset",
                ' to address: ',
                transaction_details:"_internalTransactionDetails":"address",
                '. The Coinbase ID: ',
                transaction_details:"externalProviderTransactionDetails":"details":"coinbase_account_id"
            )

            WHEN transfer_type IN ('CRYPTO_WITHDRAWALS_SETTLED') 
            AND external_provider != 'COINBASE' 
            THEN concat(
                'Withdrew ',
                transaction_details:"_internalTransactionDetails":"asset",
                ' to address: ',
                transaction_details:"_internalTransactionDetails":"address"
            )

            WHEN transfer_type IN (
                'CRYPTO_WITHDRAWALS_PENDING',
                'CRYPTO_WITHDRAWALS_REFUNDED',
                'CRYPTO_WITHDRAWALS_QUEUED_FOR_REFUND',
                'CRYPTO_WITHDRAWALS_LRC_REVIEW',
                'CRYPTO_WITHDRAWALS_REVERSAL',
                'CRYPTO_WITHDRAWALS_FAILED',
                'CRYPTO_WITHDRAWALS_SUBMITTED',
                'CRYPTO_WITHDRAWALS_PAYMENT_SUBMITTED'
            )
            THEN concat(
                'Attempted withdrawal of ',
                transaction_details:"_internalTransactionDetails":"asset",
                ' to address: ',
                transaction_details:"_internalTransactionDetails":"address"
            ) end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from
        all_transfer_ids
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
        true as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when is_transfer_reversal = true then concat(hlo_table, '_', 'REVERSAL')
            else concat(hlo_table, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'asset', coalesce(asset, try_parse_json('NULL')),
            'address', coalesce(address,try_parse_json('NULL')),
            'transactionHash', coalesce(transaction_hash,try_parse_json('NULL')),
            'fee',coalesce(fee::number(10,2),try_parse_json('NULL')),
            'reversalForOriginalTransferId',coalesce(transfer_id,try_parse_json('NULL')),
            'reversalForOriginalJournalId',coalesce(hlo_journal_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',coalesce(PROVIDER_DETAILS,try_parse_json('NULL'))
        ) as transaction_details,
        case when 
        transfer_type IN ('CRYPTO_WITHDRAWALS_SETTLED') 
            AND external_provider = 'COINBASE' 
            THEN concat(
                'Withdrew ',
                transaction_details:"_internalTransactionDetails":"asset",
                ' to address: ',
                transaction_details:"_internalTransactionDetails":"address",
                '. The Coinbase ID: ',
                transaction_details:"externalProviderTransactionDetails":"details":"coinbase_account_id"
            )

            WHEN transfer_type IN ('CRYPTO_WITHDRAWALS_SETTLED') 
            AND external_provider != 'COINBASE' 
            THEN concat(
                'Withdrew ',
                transaction_details:"_internalTransactionDetails":"asset",
                ' to address: ',
                transaction_details:"_internalTransactionDetails":"address"
            )

            WHEN transfer_type IN (
                'CRYPTO_WITHDRAWALS_PENDING',
                'CRYPTO_WITHDRAWALS_REFUNDED',
                'CRYPTO_WITHDRAWALS_QUEUED_FOR_REFUND',
                'CRYPTO_WITHDRAWALS_LRC_REVIEW',
                'CRYPTO_WITHDRAWALS_REVERSAL',
                'CRYPTO_WITHDRAWALS_FAILED',
                'CRYPTO_WITHDRAWALS_SUBMITTED',
                'CRYPTO_WITHDRAWALS_PAYMENT_SUBMITTED'
            )
            THEN concat(
                'Attempted withdrawal of ',
                transaction_details:"_internalTransactionDetails":"asset",
                ' to address: ',
                transaction_details:"_internalTransactionDetails":"address"
            ) end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id

    from
        all_transfer_ids
    where
        is_original_transfer_reversed = true
        and reverse_transfer_hlo_id is not null
)

select *
from
    transfer_ids
union
select *
from
    reverse_transfer_ids
