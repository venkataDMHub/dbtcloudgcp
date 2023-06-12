{{ config(materialized='table', dist='transfer_id', schema='looker') }}

{% set calculation_1e8 = ('BTC','ETH','SOL' ,'LUNA','AVAX', 'LTC') %}
{% set calculation_1e2 = ('USDC','DOGE' ,'ADA','DOT','MATIC', 'LINK', 'UNI') %}


WITH parallel_rates AS (

    SELECT DISTINCT
        date,
        rate,
        currency
    FROM chipper.utils.ngn_usd_parallel_market_rates

), exchante_rates as (

select currency,timestamp, rate
from chipper.{{ var("core_public") }}.exchange_rates 
qualify timestamp = max(timestamp) over (partition by currency,cast(timestamp as date) )

), crypto_transfers as (

Select 
    transfers.transfer_id, 
    transfers.is_original_transfer_reversed, 
    transfers.is_transfer_reversal, 
    transfers.journal_id,
    transfers.journal_type,
    transfers.hlo_id,
    transfers.hlo_table,
    transfers.hlo_status,
    transfers.transfer_type, 
    transfers.transfer_status, 
    transfers.transfer_created_at,
    transfers.transfer_updated_at,
    transfers.hlo_created_at,
    transfers.hlo_updated_at, 
    transfers.origin_currency, 
    transfers.origin_amount,
    transfers.origin_rate_id,
    transfers.origin_rate, 
    transfers.origin_amount_in_USD, 
    transfers.exchange_rate_fee_percentage, 
    transfers.exchange_rate, 
    transfers.corridor, 
    transfers.destination_currency,
    transfers.destination_amount,
    transfers.destination_rate_id,
    transfers.destination_rate,
    transfers.destination_amount_in_usd,
    transfers.flat_fee_currency,
    transfers.flat_fee_amount,
    transfers.flat_fee_rate,
    transfers.flat_fee_amount_in_USD,
    base_modification_percentage,
    coalesce(transfers.outgoing_user_id, transfers.incoming_user_id) as user_id,
    origin_parallel.rate AS origin_ngn_parallel_rate,
    destination_parallel.rate AS destination_ngn_parallel_rate,
    transfers.origin_amount_in_USD - transfers.destination_amount_in_usd as gains_and_losses, 
    gains_and_losses/nullif(transfers.origin_amount_in_USD, 0) as gains_as_percent_of_received,
    gains_and_losses/nullif(transfers.destination_amount_in_usd, 0) as gains_as_percent_of_given,
    volume.adjusted_transaction_volume_in_usd as usd_tpv,
    case
            when transfers.origin_currency = 'NGN' then transfers.origin_amount / origin_parallel.rate
            when transfers.origin_currency != 'NGN' then transfers.origin_amount_in_USD
            else NULL
        end as origin_amount_in_USD_parallel,
    case
            when transfers.destination_currency = 'NGN' then transfers.destination_amount / destination_parallel.rate
            when transfers.destination_currency != 'NGN' then transfers.destination_amount_in_USD
            else NULL
        end as destination_amount_in_USD_parallel,
    case
        when transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED'
            then transfers.destination_currency
        else transfers.origin_currency 
    end as cryptocurrency, 
      case
        when transfers.transfer_type in ('CRYPTO_DEPOSITS_SETTLED','CRYPTO_WITHDRAWALS_SETTLED' ) 
            then null 
        when transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED'
            then transfers.origin_currency
        when transfers.transfer_type = 'ASSET_TRADES_SELL_SETTLED'
            then transfers.destination_currency
        else null
    end as fiat_currency,  
    Case 
        when transfers.transfer_type in ('ASSET_TRADES_BUY_SETTLED','CRYPTO_DEPOSITS_SETTLED') 
            and transfers.destination_currency in {{calculation_1e8}}
                then transfers.destination_amount/1e8
        when transfers.transfer_type  in ('ASSET_TRADES_BUY_SETTLED','CRYPTO_DEPOSITS_SETTLED') 
            and transfers.destination_currency in {{calculation_1e2}}
                 then transfers.destination_amount / 1e2
        when transfers.transfer_type in ('ASSET_TRADES_SELL_SETTLED','CRYPTO_WITHDRAWALS_SETTLED') 
            and transfers.origin_currency in {{calculation_1e8}}
                then transfers.origin_amount / 1e8
        when transfers.transfer_type in ('ASSET_TRADES_SELL_SETTLED','CRYPTO_WITHDRAWALS_SETTLED') 
            and transfers.origin_currency in {{calculation_1e2}}
                then transfers.origin_amount / 1e2
    END AS crypto_Units,
    Case 
        when transfers.transfer_type in ('ASSET_TRADES_BUY_SETTLED','CRYPTO_DEPOSITS_SETTLED')  
            and transfers.destination_currency  in {{calculation_1e8}} 
                then transfers.origin_amount_in_USD/(transfers.destination_amount / 1e8)
        when transfers.transfer_type in ('ASSET_TRADES_BUY_SETTLED','CRYPTO_DEPOSITS_SETTLED')  
            and transfers.destination_currency in {{calculation_1e2}} 
            then transfers.origin_amount_in_USD /(transfers.destination_amount / 1e2)
     when transfers.transfer_type in ('ASSET_TRADES_SELL_SETTLED','CRYPTO_WITHDRAWALS_SETTLED') 
            and transfers.origin_currency in {{calculation_1e8}}
                then transfers.destination_amount_in_USD /(transfers.origin_amount/ 1e8)
     when transfers.transfer_type in ('ASSET_TRADES_SELL_SETTLED','CRYPTO_WITHDRAWALS_SETTLED') 
        and transfers.origin_currency in {{calculation_1e2}}
            then transfers.destination_amount_in_USD/(transfers.origin_amount / 1e2)
    END AS User_Unit_Price_USD,
    Case 
        when transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED' 
            and transfers.destination_currency in {{calculation_1e8}} 
                then transfers.origin_amount/(transfers.destination_amount/ 1e8)
        when transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED' 
            and transfers.destination_currency in {{calculation_1e2}}
                then transfers.origin_amount/(transfers.destination_amount/ 1e2)
        when transfers.transfer_type  = 'ASSET_TRADES_SELL_SETTLED' 
            and transfers.origin_currency in {{calculation_1e8}} 
                then transfers.destination_amount/(transfers.origin_amount/ 1e8)
        when transfers.transfer_type = 'ASSET_TRADES_SELL_SETTLED' 
            and  transfers.origin_currency in {{calculation_1e2}}
                then transfers.destination_amount/(transfers.origin_amount/ 1e2)
        when transfers.transfer_type = 'CRYPTO_DEPOSITS_SETTLED'  
            and transfers.destination_currency IN {{calculation_1e8}}  
                then (transfers.origin_rate * 1e8)/rate.Rate
        when transfers.transfer_type = 'CRYPTO_DEPOSITS_SETTLED'
             and transfers.destination_currency IN {{calculation_1e2}} 
                then (transfers.origin_rate * 1e2)/rate.Rate
        when transfers.transfer_type = 'CRYPTO_WITHDRAWALS_SETTLED' 
            and transfers.origin_currency IN {{calculation_1e8}}  
                then (transfers.destination_rate * 1e8)/rate.Rate
        when transfers.transfer_type = 'CRYPTO_WITHDRAWALS_SETTLED' 
            and transfers.origin_currency IN {{calculation_1e2}} 
                then (transfers.destination_rate * 1e2)/rate.rate
    END AS User_Unit_Price_Local,
    Case 
        when transfers.transfer_type IN ('ASSET_TRADES_BUY_SETTLED','CRYPTO_DEPOSITS_SETTLED') 
            and transfers.destination_currency IN {{calculation_1e8}}
                then transfers.destination_rate * 1e8
        when transfers.transfer_type IN ('ASSET_TRADES_BUY_SETTLED','CRYPTO_DEPOSITS_SETTLED') 
            and transfers.destination_currency IN {{calculation_1e2}} 
                then transfers.destination_rate * 1e2
        when transfers.transfer_type IN('ASSET_TRADES_SELL_SETTLED','CRYPTO_WITHDRAWALS_SETTLED') 
            and  transfers.origin_currency IN {{calculation_1e8}}
                then transfers.origin_rate * 1e8
        when transfers.transfer_type IN('ASSET_TRADES_SELL_SETTLED','CRYPTO_WITHDRAWALS_SETTLED') 
            and  transfers.origin_currency IN {{calculation_1e2}}
                then transfers.origin_rate * 1e2
    END AS Fair_Value_Unit_Price_USD,
    Case 
        when transfers.transfer_type = 'ASSET_TRADES_BUY_SETTLED' 
            then Fair_Value_Unit_Price_USD / transfers.origin_rate
        when transfers.transfer_type = 'ASSET_TRADES_SELL_SETTLED' 
            then Fair_Value_Unit_Price_USD / transfers.destination_rate
        when transfers.transfer_type in ('CRYPTO_DEPOSITS_SETTLED','CRYPTO_WITHDRAWALS_SETTLED') 
            then Fair_Value_Unit_Price_USD/rate.rate 
End as Fair_Value_Unit_Price_Local,
case 
    when transfers.origin_currency = 'NGN' 
        then origin_amount_in_USD_parallel - transfers.destination_amount_in_USD 
    when transfers.destination_currency = 'NGN' 
        then transfers.origin_amount_in_USD - destination_amount_in_USD_parallel
    else null
End as NGN_difference_receive_give_in_USD,
case 
    when transfers.origin_currency = 'NGN' 
        then origin_amount_in_USD_parallel  - transfers.origin_amount_in_USD 
    when transfers.destination_currency = 'NGN' 
        then destination_amount_in_USD_parallel - transfers.destination_amount_in_USD
    else null
End as NGN_Amount_Parallel_Official_diff_in_USD,
case 
    when transfers.origin_currency = 'NGN' 
        and transfers.destination_currency IN {{calculation_1e8}}
            then transfers.destination_rate * 1e8 * origin_parallel.rate
    when transfers.origin_currency = 'NGN' 
        and transfers.destination_currency IN {{calculation_1e2}} 
            then transfers.destination_rate * 1e2 * origin_parallel.rate
    when transfers.destination_currency = 'NGN' 
        and transfers.origin_currency IN {{calculation_1e8}} 
            then transfers.origin_rate * 1e8 * destination_parallel.rate
    when transfers.destination_currency = 'NGN' 
        and transfers.origin_currency IN {{calculation_1e2}} 
            then transfers.origin_rate * 1e2 * destination_parallel.rate
    else null
End as NGN_Parallel_Unit_Price_in_NGN
 from {{ref('expanded_transfers')}} transfers
left join chipper.dbt_transformations.aggregated_transaction_volume as volume
    on concat_ws('-', transfers.hlo_table, transfers.hlo_id) = volume.hlo_table_with_id
 LEFT JOIN parallel_rates AS origin_parallel
        ON transfers.origin_currency = origin_parallel.currency
            AND cast(transfers.hlo_updated_at AS DATE) = cast(origin_parallel.date AS DATE)
LEFT JOIN parallel_rates AS destination_parallel
    ON transfers.destination_currency = destination_parallel.currency
        AND cast(transfers.hlo_updated_at AS DATE) = cast(destination_parallel.date AS DATE)
LEFT JOIN {{ref('expanded_users')}}  AS users 
    on COALESCE (transfers.INCOMING_USER_ID, transfers.OUTGOING_USER_ID) = users.User_ID
LEFT JOIN exchante_rates  AS rate
    on cast (transfers.HLO_UPDATED_AT as Date) = cast(rate.timestamp as date)
    and users.primary_currency = rate.currency
 Where transfers.hlo_status in ('COMPLETED', 'SETTLED')
    and transfers.is_original_transfer_reversed = False
    and transfers.transfer_type in ('ASSET_TRADES_BUY_SETTLED', 'ASSET_TRADES_SELL_SETTLED', 'CRYPTO_DEPOSITS_SETTLED','CRYPTO_WITHDRAWALS_SETTLED' )
)

Select *
from crypto_transfers
