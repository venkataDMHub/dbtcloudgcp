{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select
        id::text as hlo_id,
        'STOCK_TRADES' as hlo_table,
        journal_id as hlo_journal_id,
        status as hlo_status,
        created_at as hlo_created_at,
        updated_at as hlo_updated_at,
        transfer_id as transfer_id,
        reverse_transfer_id as reverse_transfer_id,
        case
            when reverse_transfer_id is null then false
            else true
        end as is_original_transfer_reversed,
        'DRIVEWEALTH' as external_provider,
        symbol,
        shares,
        status,
        ADMIN_ID,
        FEE_CURRENCY,
        FEE_AMOUNT,
        amount_in_usd,
        ORDER_RESPONSE,
        DIVIDEND_PAYLOAD,
        position, 
    case when order_response is not null then order_response:id::varchar
         when dividend_payload is not null then dividend_payload:transaction:finTranID::varchar
    end
    as external_provider_transaction_id,
    case when position in ('SELL', 'DIV', 'CANCELLED') then user_id else null end as outgoing_user_id,
    case when position in ('BUY', 'REWARD', 'CANCELLED', 'DIVTAX') then user_id else null end as incoming_user_id
    from "CHIPPER".{{ var("core_public") }}."STOCK_TRADES"
    where transfer_id is not null
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
            when
                is_transfer_reversal = true then concat(hlo_table, '_', 'REVERSAL')
            else concat(hlo_table, '_', position, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'symbol', coalesce(symbol, try_parse_json('NULL')),
            'shares', coalesce(shares::decimal(38,12),try_parse_json('NULL')),
            'statusMessage', coalesce(status,try_parse_json('NULL')),
            'adminId', coalesce(ADMIN_ID,try_parse_json('NULL')),
            'feeCurrency', coalesce(FEE_CURRENCY,try_parse_json('NULL')),
            'feeAmount', coalesce(FEE_AMOUNT::number(10,2),try_parse_json('NULL')),
            'amountInUsd', coalesce(amount_in_usd::number(10,2),try_parse_json('NULL')),
            'usdPricePerShare', ifnull((amount_in_usd / nullif(shares, 0))::number(10,2), 0::number(10,2)),
            'originalTransferForReverseTransferId',coalesce(REVERSE_TRANSFER_ID,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',coalesce(ORDER_RESPONSE,DIVIDEND_PAYLOAD, try_parse_json('NULL'))
        ) as transaction_details,
    CASE WHEN transfer_type IN (
        'STOCK_TRADES_BUY_SETTLED',
        'STOCK_TRADES_SELL_SETTLED',
        'STOCK_TRADES_REWARD_SETTLED',
        'STOCK_TRADES_MERGER_EXCHANGE_STOCK_CASH_SETTLED',
        'STOCK_TRADES_BUY_PARTIAL_FILL',
        'STOCK_TRADES_SELL_PARTIAL_FILL',
        'STOCK_TRADES_SELL_ORDER_CREATED',
        'STOCK_TRADES_SELL_ORDER_CREATED'


      )
      THEN concat (
        COALESCE(transaction_details:"externalProviderTransactionDetails":"side",' '),
        ' ',
        transaction_details:"_internalTransactionDetails":"shares",
        'shares of ',
        transaction_details:"_internalTransactionDetails":"symbol",
        'worth $',
        transaction_details:"_internalTransactionDetails":"amountInUsd",
        ' at $',
        transaction_details:"_internalTransactionDetails":"usdPricePerShare",
        ' per share. The',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"lastMarket",' '),
        ' Account ID: ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"accountID",''),
        ' with Account Number: ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"accountNo",''),
        '. The Order No: ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"orderNo",'')
      )

      WHEN transfer_type IN (
        'STOCK_TRADES_DIVTAX_SETTLED',
        'STOCK_TRADES_DIV_SETTLED',
        'STOCK_TRADES_DIV_PARTIAL_FILL',
        'STOCK_TRADES_DIVTAX_PARTIAL_FILL',
        'STOCK_TRADES_DIVTAX_FAILED',
        'STOCK_TRADES_DIV_FAILED'

      )
      THEN concat (
        transaction_details:"externalProviderTransactionDetails":"transaction":"finTranTypeID",
        ' - ',
        transaction_details:"_internalTransactionDetails":"symbol",
        'worth $',
        transaction_details:"_internalTransactionDetails":"amountInUsd",
        '. The Drivewealth Account ID:  ',
        transaction_details:"externalProviderTransactionDetails":"accountID",
        ' with Account Number: ',
        transaction_details:"externalProviderTransactionDetails":"accountNo"
      )

      WHEN transfer_type IN (
        'STOCK_TRADES_BUY_PENDING',
        'STOCK_TRADES_SELL_CANCELLED',
        'STOCK_TRADES_CANCELLED_CANCELLED',
        'STOCK_TRADES_CANCELLED_FAILED',
        'STOCK_TRADES_BUY_CANCELLED',
        'STOCK_TRADES_BUY_FAILED',
        'STOCK_TRADES_SELL_FAILED',
        'STOCK_TRADES_BUY_REJECTED',
        'STOCK_TRADES_SELL_REJECTED',
        'STOCK_TRADES_REVERSAL'
      )
      THEN concat (
        'Attempted ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"side",''),
        ' ',
        transaction_details:"_internalTransactionDetails":"shares",
        'cshares of ',
        transaction_details:"_internalTransactionDetails":"symbol",
        'worth $',
        transaction_details:"_internalTransactionDetails":"amountInUsd",
        ' at $',
        transaction_details:"_internalTransactionDetails":"usdPricePerShare",
        ' per share.'
      ) end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from all_transfer_ids
),

reverse_transfer_ids as (
    select
        hlo_id,
        hlo_table,
        hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        true as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        case
            when
                is_transfer_reversal = true then concat(hlo_table, '_', 'REVERSAL')
            else concat(hlo_table, '_', position, '_', hlo_status)
        end as transfer_type,
        object_construct(
        '_internalTransactionDetails',object_construct(
            'symbol', coalesce(symbol, try_parse_json('NULL')),
            'shares', coalesce(shares::decimal(38,12),try_parse_json('NULL')),
            'statusMessage', coalesce(status,try_parse_json('NULL')),
            'adminId', coalesce(ADMIN_ID,try_parse_json('NULL')),
            'feeCurrency', coalesce(FEE_CURRENCY,try_parse_json('NULL')),
            'feeAmount', coalesce(FEE_AMOUNT::number(10,2),try_parse_json('NULL')),
            'amountInUsd', coalesce(amount_in_usd::number(10,2),try_parse_json('NULL')),
            'usdPricePerShare', ifnull((amount_in_usd / nullif(shares, 0))::number(10,2), 0::number(10,2)),
            'reversalForOriginalTransferId',coalesce(transfer_id,try_parse_json('NULL')),
            'reversalForOriginalJournalId',coalesce(hlo_journal_id,try_parse_json('NULL'))
        ),
        'externalProviderTransactionDetails',coalesce(ORDER_RESPONSE,DIVIDEND_PAYLOAD, try_parse_json('NULL'))
    ) as transaction_details,
    CASE WHEN transfer_type IN (
        'STOCK_TRADES_BUY_SETTLED',
        'STOCK_TRADES_SELL_SETTLED',
        'STOCK_TRADES_REWARD_SETTLED',
        'STOCK_TRADES_MERGER_EXCHANGE_STOCK_CASH_SETTLED',
        'STOCK_TRADES_BUY_PARTIAL_FILL',
        'STOCK_TRADES_SELL_PARTIAL_FILL',
        'STOCK_TRADES_SELL_ORDER_CREATED',
        'STOCK_TRADES_SELL_ORDER_CREATED'

      )
      THEN concat (
        COALESCE(transaction_details:"externalProviderTransactionDetails":"side",' '),
        ' ',
        transaction_details:"_internalTransactionDetails":"shares",
        'shares of ',
        transaction_details:"_internalTransactionDetails":"symbol",
        'worth $',
        transaction_details:"_internalTransactionDetails":"amountInUsd",
        ' at $',
        transaction_details:"_internalTransactionDetails":"usdPricePerShare",
        ' per share. The',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"lastMarket",' '),
        ' Account ID: ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"accountID",''),
        ' with Account Number: ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"accountNo",''),
        '. The Order No: ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"orderNo",'')
      )

      WHEN transfer_type IN (
        'STOCK_TRADES_DIVTAX_SETTLED',
        'STOCK_TRADES_DIV_SETTLED',
        'STOCK_TRADES_DIV_PARTIAL_FILL',
        'STOCK_TRADES_DIVTAX_PARTIAL_FILL',
        'STOCK_TRADES_DIVTAX_FAILED',
        'STOCK_TRADES_DIV_FAILED'

      )
      THEN concat (
        transaction_details:"externalProviderTransactionDetails":"transaction":"finTranTypeID",
        ' - ',
        transaction_details:"_internalTransactionDetails":"symbol",
        'worth $',
        transaction_details:"_internalTransactionDetails":"amountInUsd",
        '. The Drivewealth Account ID:  ',
        transaction_details:"externalProviderTransactionDetails":"accountID",
        ' with Account Number: ',
        transaction_details:"externalProviderTransactionDetails":"accountNo"
      )

      WHEN transfer_type IN (
        'STOCK_TRADES_BUY_PENDING',
        'STOCK_TRADES_SELL_CANCELLED',
        'STOCK_TRADES_CANCELLED_CANCELLED',
        'STOCK_TRADES_CANCELLED_FAILED',
        'STOCK_TRADES_BUY_CANCELLED',
        'STOCK_TRADES_BUY_FAILED',
        'STOCK_TRADES_SELL_FAILED',
        'STOCK_TRADES_BUY_REJECTED',
        'STOCK_TRADES_SELL_REJECTED',
        'STOCK_TRADES_REVERSAL'
      )
      THEN concat (
        'Attempted ',
        COALESCE(transaction_details:"externalProviderTransactionDetails":"side",''),
        ' ',
        transaction_details:"_internalTransactionDetails":"shares",
        'cshares of ',
        transaction_details:"_internalTransactionDetails":"symbol",
        'worth $',
        transaction_details:"_internalTransactionDetails":"amountInUsd",
        ' at $',
        transaction_details:"_internalTransactionDetails":"usdPricePerShare",
        ' per share.'
      ) end as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id

    from all_transfer_ids
    where is_original_transfer_reversed = true
)

select *
from transfer_ids
union
select *
from reverse_transfer_ids
