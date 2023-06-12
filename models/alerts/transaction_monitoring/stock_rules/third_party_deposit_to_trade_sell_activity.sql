{{ config(materialized="table") }}

{% set stock_amount_threshold = 100 %}
{% set day_interval = "1 day" %}
{% set transfer_type = "PAYMENTS_P2P_SETTLED" %}
{% set stock_position = "BUY" %}


with
    p2p_payment as (

        select

            ledgers.journal_id,
            ledgers.main_party_user_id,
            ledgers.ledger_currency,
            ledgers.ledger_amount,
            ledgers.ledger_amount_in_usd,
            ledgers.ledger_timestamp,
            ledgers.journal_type,
            ledgers.transfer_id,

            ledgers.main_party_user_id as recipient_id,
            ledgers.counter_party_user_id as sender_id,
            ledgers.hlo_status as status,
            ledgers.hlo_created_at as payment_created_at,
            ledgers.hlo_updated_at as payment_updated_at,
            'P2P' as payment_grouping

        from {{ ref("expanded_ledgers") }} as ledgers

        where
            ledgers.transfer_type = '{{transfer_type}}'
            and ledgers.hlo_status = 'SETTLED'
            and ledgers.ledger_amount >= '0'
    ),

    payment_details as (

        select
            requests.id,
            requests.journal_id,
            requests.note,
            requests.created_at,
            requests.updated_at,
            requests.transfer_id,
            requests.sender_id,
            requests.recipient_id,
            requests.status,

            concat(senders.first_name, ' ', senders.last_name) as sender_name,
            concat(receivers.first_name, ' ', receivers.last_name) as recipient_name,

            case
                when senders.is_business = 'FALSE' and receivers.is_business = 'FALSE'
                then 'p2p_request'
                else 'other'
            end as request_grouping

        from chipper.{{ var("core_public") }}.requests as requests

        left join
            chipper.{{ var("core_public") }}.users as senders
            on requests.sender_id = senders.id

        left join
            chipper.{{ var("core_public") }}.users as receivers
            on requests.recipient_id = receivers.id

        where
            requests.status = 'SETTLED'
            and senders.is_business = 'FALSE'
            and receivers.is_business = 'FALSE'

    ),

    p2p_request as (

        select

            ledgers.journal_id,
            ledgers.main_party_user_id,
            ledgers.ledger_currency,
            ledgers.ledger_amount,
            ledgers.ledger_amount_in_usd,
            ledgers.ledger_timestamp,
            ledgers.journal_type,
            ledgers.transfer_id,

            details.sender_id,
            details.recipient_id,
            details.status,
            details.created_at as request_created_at,
            details.updated_at as request_updated_at,
            details.request_grouping

        from {{ ref("expanded_ledgers") }} as ledgers

        inner join
            payment_details as details

            on ledgers.journal_id = details.journal_id
            and ledgers.main_party_user_id = details.sender_id

    ),

    p2p_transfer as (

        select
            payment.journal_id,
            payment.main_party_user_id,
            payment.ledger_currency,
            payment.ledger_amount,
            payment.ledger_amount_in_usd,
            payment.ledger_timestamp,
            payment.journal_type,
            payment.transfer_id,
            payment.sender_id,
            payment.recipient_id,
            payment.status,
            'P2P_Payment' as p2p_type
        from p2p_payment as payment

        union all

        select
            request.journal_id,
            request.main_party_user_id,
            request.ledger_currency,
            request.ledger_amount,
            request.ledger_amount_in_usd,
            request.ledger_timestamp,
            request.journal_type,
            request.transfer_id,
            request.sender_id,
            request.recipient_id,
            request.status,
            'P2P_Request' as p2p_type
        from p2p_request as request
    ),

    stock_trade_sell as (

        select
            stocks.journal_id as stock_trade_journal_id,
            stocks.transfer_id as stock_trade_transfer_id,
            stocks.user_id as stock_trade_user_id,
            stocks.status as stock_trade_status,
            stocks.position,
            stocks.symbol,
            stocks.currency as stock_trade_currency,
            stocks.amount as stock_trade_amount,
            stocks.shares,
            stocks.amount_in_usd,
            stocks.created_at as stock_trade_created_at,
            transfers.journal_id,
            transfers.main_party_user_id,
            transfers.ledger_currency,
            transfers.ledger_amount,
            transfers.ledger_amount_in_usd,
            transfers.ledger_timestamp,
            transfers.journal_type,
            transfers.transfer_id,
            transfers.sender_id,
            transfers.recipient_id,
            transfers.status,
            transfers.p2p_type

        from chipper.{{ var("core_public") }}.stock_trades as stocks

        left join
            p2p_transfer as transfers
            on stocks.user_id = transfers.main_party_user_id
            and cast(transfers.ledger_timestamp as date) between (
                cast(stocks.created_at as date) - interval '{{day_interval}}'
            ) and cast(stocks.created_at as date)
            and transfers.ledger_timestamp <= stocks.created_at
            and stocks.amount
            between transfers.ledger_amount * 0.8 and transfers.ledger_amount * 1.25
            and stocks.currency = transfers.ledger_currency

        where
            stocks.status = 'SETTLED'
            and stocks.position in ('{{stock_position}}')
            and stocks.amount_in_usd >= {{ stock_amount_threshold }}
            and transfers.journal_id is not null

        order by stocks.user_id, stocks.created_at
    )

select
    main_party_user_id as user_id,
    stock_trade_created_at as triggered_at,
    array_construct(transfer_id) as list_of_txns
from stock_trade_sell
