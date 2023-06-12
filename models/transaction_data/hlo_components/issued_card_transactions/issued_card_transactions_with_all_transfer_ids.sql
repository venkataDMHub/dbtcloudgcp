{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select
        issued_card_transactions.id::text as hlo_id,
        'ISSUED_CARD_TRANSACTIONS' as hlo_table,
        issued_card_transactions.type as hlo_type,
        issued_card_transactions.journal_id as hlo_journal_id,
        issued_card_transactions.status as hlo_status,
        issued_card_transactions.created_at as hlo_created_at,
        issued_card_transactions.updated_at as hlo_updated_at,
        issued_card_transactions.transfer_id as transfer_id,
        issued_card_transactions.reverse_transfer_id as reverse_transfer_id,
        case when issued_card_transactions.reverse_transfer_id is null then false
            else true
        end as is_original_transfer_reversed,
        issued_cards.id as issued_cards_id,
        card_last_four,
        expiry_date,
        card_status,
        name_on_card,
        phone,
        issued_cards.currency as issued_cards_currency,
        issued_cards.country as issued_cards_country,
        issued_cards.provider_card_id as issued_cards_provider_card_id,
        entry_type,
        description,
        admin_id,
        error_message,
        replacement_card_id,
        tokenized_card_number,
--        tokenized_expiry_date,
        tokenized_cvv,
        issued_cards.provider_details as issued_cards_provider_details,
        issued_card_transactions.provider_details as issued_card_transactions_provider_details,
        card_issuer as external_provider,
        provider_transaction_id::varchar as external_provider_transaction_id,
        case when issued_card_transactions.reverse_transfer_id is null then  issued_card_transactions.user_id else null end as outgoing_user_id,
        case when issued_card_transactions.reverse_transfer_id is not null then issued_card_transactions.user_id else null end as incoming_user_id,

        iff(reversals_on_disbursements.transfer_id is null, false, true) as is_reverse_transfer_id_disbursement,
        iff(is_reverse_transfer_id_disbursement = false, issued_card_transactions.id::text, reversals_on_disbursements.id::text) as reverse_transfer_hlo_id,
        iff(is_reverse_transfer_id_disbursement = false, 'ISSUED_CARD_TRANSACTIONS', 'DISBURSEMENTS') as reverse_transfer_hlo_table,
        iff(is_reverse_transfer_id_disbursement = false, reverse_transfers.journal_id, reversals_on_disbursements.journal_id) as reverse_transfer_journal_id,
        iff(is_reverse_transfer_id_disbursement = false, issued_card_transactions.status, reversals_on_disbursements.status) as reverse_transfer_hlo_status,
        iff(is_reverse_transfer_id_disbursement = false, issued_card_transactions.created_at, reversals_on_disbursements.created_at) as reverse_transfer_hlo_created_at,
        iff(is_reverse_transfer_id_disbursement = false, issued_card_transactions.updated_at, reversals_on_disbursements.updated_at) as reverse_transfer_hlo_updated_at

        from
            "CHIPPER".{{ var("core_public") }}."ISSUED_CARD_TRANSACTIONS" 
            left join "CHIPPER".{{ var("core_public") }}."ISSUED_CARDS"
        on (
            issued_card_transactions.card_id = issued_cards.id 
            and issued_card_transactions.provider_card_id = issued_cards.provider_card_id
        )
        left join "CHIPPER".{{ var("core_public") }}."TRANSFERS" as reverse_transfers
            on issued_card_transactions.reverse_transfer_id = reverse_transfers.id
        left join "CHIPPER".{{ var("core_public") }}."DISBURSEMENTS" as reversals_on_disbursements
            on issued_card_transactions.reverse_transfer_id = reversals_on_disbursements.transfer_id
        where issued_card_transactions.transfer_id is not null
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
        {% endif %}


),

transfer_ids as (
    select
        hlo_id,
        hlo_table,
        hlo_type,
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
            when
                is_transfer_reversal = true then concat(hlo_table, '_', 'REVERSAL')
            else concat(hlo_table, '_', hlo_type, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails', object_construct(
            'reversalForOriginalTransferId', coalesce(transfer_id, try_parse_json('NULL')),
            'reversalForOriginalJournalId', coalesce(hlo_journal_id, try_parse_json('NULL')),
            'cardId', coalesce(issued_cards_id, try_parse_json('NULL')),
            'cardLastFour', coalesce(card_last_four, try_parse_json('NULL')),
            'cardExpiryDate', coalesce(expiry_date, try_parse_json('NULL')),
            'cardStatus', coalesce(card_status, try_parse_json('NULL')),
            'nameOnCard', coalesce(name_on_card, try_parse_json('NULL')),
            'userPhoneNumber', coalesce(phone, try_parse_json('NULL')),
            'cardCurrency', coalesce(issued_cards_currency, try_parse_json('NULL')),
            'cardCountry', coalesce(issued_cards_country, try_parse_json('NULL')),
            'providerCardId', coalesce(issued_cards_provider_card_id, try_parse_json('NULL')),
            'entryType', coalesce(entry_type, try_parse_json('NULL')),
            'description', coalesce(description, try_parse_json('NULL')),
            'adminId', coalesce(admin_id, try_parse_json('NULL')),
            'errorMessage', coalesce(error_message, try_parse_json('NULL')),
            'replacementCardId', coalesce(replacement_card_id, try_parse_json('NULL')),
            'tokenizedCardNumber', coalesce(tokenized_card_number, try_parse_json('NULL')),
--            'tokenizedCardExpiryDate', coalesce(tokenized_expiry_date, try_parse_json('NULL')),
            'tokenizedCvv', coalesce(tokenized_cvv, try_parse_json('NULL'))
        ),

        'externalProviderCardDetails', coalesce(issued_cards_provider_details, try_parse_json('NULL')),
        'externalProviderTransactionDetails', coalesce(issued_card_transactions_provider_details, try_parse_json('NULL'))
        ) as transaction_details,
        CASE WHEN transfer_type IN (
            'ISSUED_CARD_TRANSACTIONS_FUNDING_COMPLETED'
        )
        THEN concat (
            'Funding Card ',
            transaction_details:"_internalTransactionDetails":"cardLastFour",
            '. The Provider Transaction ID:',
            COALESCE(EXTERNAL_PROVIDER_TRANSACTION_ID,'NULL')
        )

        WHEN transfer_type IN (
            'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_COMPLETED'
        )
        THEN concat (
            'Withdrawal from Card ',
            transaction_details:"_internalTransactionDetails":"cardLastFour",
            '. The Provider Transaction ID:',
            COALESCE(EXTERNAL_PROVIDER_TRANSACTION_ID,'NULL')
        )

        WHEN transfer_type IN (
            'ISSUED_CARD_TRANSACTIONS_FUNDING_NEW',
            'ISSUED_CARD_TRANSACTIONS_REVERSAL',
            'ISSUED_CARD_TRANSACTIONS_FUNDING_REFUNDED',
            'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_FAILED',
            'ISSUED_CARD_TRANSACTIONS_FUNDING_FAILED',
            'ISSUED_CARD_TRANSACTIONS_REFUNDED',
            'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_REFUNDED'
        )
        THEN concat (
            'Attempted funding of issued card - Funding Card ',
            COALESCE(transaction_details:"_internalTransactionDetails":"cardLastFour",'NULL')
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
        hlo_type,
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
            when
                is_transfer_reversal = true then concat(hlo_table, '_', 'REVERSAL')
            else concat(hlo_table, '_', hlo_type, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails', object_construct(
            'reversalForOriginalTransferId', coalesce(transfer_id, try_parse_json('NULL')),
            'reversalForOriginalJournalId', coalesce(hlo_journal_id, try_parse_json('NULL')),
            'cardId', coalesce(issued_cards_id, try_parse_json('NULL')),
            'cardLastFour', coalesce(card_last_four, try_parse_json('NULL')),
            'cardExpiryDate', coalesce(expiry_date, try_parse_json('NULL')),
            'cardStatus', coalesce(card_status, try_parse_json('NULL')),
            'nameOnCard', coalesce(name_on_card, try_parse_json('NULL')),
            'userPhoneNumber', coalesce(phone, try_parse_json('NULL')),
            'cardCurrency', coalesce(issued_cards_currency, try_parse_json('NULL')),
            'cardCountry', coalesce(issued_cards_country, try_parse_json('NULL')),
            'providerCardId', coalesce(issued_cards_provider_card_id, try_parse_json('NULL')),
            'entryType', coalesce(entry_type, try_parse_json('NULL')),
            'description', coalesce(description, try_parse_json('NULL')),
            'adminId', coalesce(admin_id, try_parse_json('NULL')),
            'errorMessage', coalesce(error_message, try_parse_json('NULL')),
            'replacementCardId', coalesce(replacement_card_id, try_parse_json('NULL')),
            'tokenizedCardNumber', coalesce(tokenized_card_number, try_parse_json('NULL')),
--            'tokenizedCardExpiryDate', coalesce(tokenized_expiry_date, try_parse_json('NULL')),
            'tokenizedCvv', coalesce(tokenized_cvv, try_parse_json('NULL'))
        ),

        'externalProviderCardDetails', coalesce(issued_cards_provider_details, try_parse_json('NULL')),
        'externalProviderTransactionDetails', coalesce(issued_card_transactions_provider_details, try_parse_json('NULL'))
        ) as transaction_details,
        CASE WHEN transfer_type IN (
            'ISSUED_CARD_TRANSACTIONS_FUNDING_COMPLETED'
        )
        THEN concat (
            'Funding Card ',
            transaction_details:"_internalTransactionDetails":"cardLastFour",
            '. The Provider Transaction ID:',
            COALESCE(EXTERNAL_PROVIDER_TRANSACTION_ID,'NULL')
        )

        WHEN transfer_type IN (
            'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_COMPLETED'
        )
        THEN concat (
            'Withdrawal from Card ',
            transaction_details:"_internalTransactionDetails":"cardLastFour",
            '. The Provider Transaction ID:',
            COALESCE(EXTERNAL_PROVIDER_TRANSACTION_ID,'NULL')
        )

        WHEN transfer_type IN (
            'ISSUED_CARD_TRANSACTIONS_FUNDING_NEW',
            'ISSUED_CARD_TRANSACTIONS_REVERSAL',
            'ISSUED_CARD_TRANSACTIONS_FUNDING_REFUNDED',
            'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_FAILED',
            'ISSUED_CARD_TRANSACTIONS_FUNDING_FAILED',
            'ISSUED_CARD_TRANSACTIONS_REFUNDED',
            'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL_REFUNDED'

        )
        THEN concat (
            'Attempted funding of issued card - Funding Card ',
            COALESCE(transaction_details:"_internalTransactionDetails":"cardLastFour",'NULL')
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
