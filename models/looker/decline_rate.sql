{{ config(materialized="table", schema="looker") }}

with
    decline_rate as (

        select distinct

            date_trunc('day', ledgers.hlo_updated_at) as transaction_settled_date,
            ledgers.ledger_currency,
            ledgers.journal_type,
            ledgers.hlo_status,
            TRANSACTION_DETAILS:externalProviderTransactionDetails:errorMessage::text as error_message_external,
            TRANSACTION_DETAILS:_internalTransactionDetails:errorMessage::text as error_message_internal,
            details.external_provider,
            --logic for "declined" transactions, per the business operations team
            case
                when
                    ledgers.hlo_status = 'DECLINED'
                    or ledgers.hlo_status = 'REJECTED'
                    or ledgers.hlo_status = 'EXTERNAL_FAILED'
                    or TRANSACTION_DETAILS:externalProviderTransactionDetails:errorMessage::text = 'Xtransbits Status check API returned Failed - Auto refunded'
                    or (TRANSACTION_DETAILS:_internalTransactionDetails:errorMessage::text = 'Failed withdrawal. Refunded.'
                        and ledgers.hlo_status = 'FAILED'
                        and details.external_provider = 'RAILSBANK'
                        )
                then 1
                else 0
            end as declined_transaction,
            --logic for "failed" transactions, per the business operations team (declined logic + any transactions with a "FAILED" status)
            case
                when
                    ledgers.hlo_status = 'DECLINED'
                    or ledgers.hlo_status = 'REJECTED'
                    or ledgers.hlo_status = 'FAILED'
                    or ledgers.hlo_status = 'EXTERNAL_FAILED'
                    or TRANSACTION_DETAILS:externalProviderTransactionDetails:errorMessage::text = 'Xtransbits Status check API returned Failed - Auto refunded'
                    or (TRANSACTION_DETAILS:_internalTransactionDetails:errorMessage::text = 'Failed withdrawal. Refunded.'
                        and ledgers.hlo_status = 'FAILED'
                        and details.external_provider = 'RAILSBANK'
                    )
                then 1
                else 0
            end as failed_transaction,
            ledgers.transfer_id

        from {{ref('expanded_ledgers')}} as ledgers

        left join
            {{ref('transaction_details')}} as details
            on ledgers.transfer_id = details.transfer_id

        left join
            chipper.{{ var("core_public") }}.assets as assets
            on ledgers.ledger_currency = assets.id

        where
            (
                (ledgers.journal_type = 'ASSET_TRADE' and assets.type = 'CRYPTO_CURRENCY')
                or ledgers.journal_type != 'ASSET_TRADE'
            )
    )

select 
    transaction_settled_date,
    ledger_currency,
    journal_type,
    hlo_status,
    error_message_external,
    error_message_internal,
    external_provider,
    sum(declined_transaction) as total_declined_transactions,
    sum(failed_transaction) as total_failed_transactions,
    count(distinct transfer_id) as total_transactions
from decline_rate
{{ dbt_utils.group_by(n=7) }}