{{ config(materialized='table', dist='user_id', schema='looker') }}

WITH parallel_rates AS (

    SELECT DISTINCT
        date,
        rate,
        currency
    FROM chipper.utils.ngn_usd_parallel_market_rates 

),

settlement_rates AS (
    SELECT DISTINCT
      date,
      origin_currency,
      destination_currency,
      settlement_rate,
      rate_type
    FROM chipper.utils.internal_settlement_rates 
),

transfer_info AS (
    SELECT
        transfers.transfer_id,
        transfer_quotes.transfer_id as transfer_quote_transfer_id,
        transfers.origin_currency,
        transfers.destination_currency,
        transfers.outgoing_user_id,
        transfers.incoming_user_id,
        transfers.exchange_rate_fee_percentage,
        transfers.base_modification_percentage,
        transfers.origin_amount,
        transfers.origin_amount_in_USD,
        transfer_quotes.origin_amount_before_fees,
        transfer_quotes.origin_amount_before_fees * transfers.origin_rate as origin_amount_before_fees_in_usd,
        origin_parallel.rate AS origin_ngn_parallel_rate,
        CASE
            WHEN transfers.origin_currency = 'NGN' THEN transfers.origin_amount / origin_parallel.rate
            WHEN transfers.origin_currency != 'NGN' THEN transfers.origin_amount_in_USD
            ELSE NULL
        END AS origin_amount_in_USD_parallel,
        CASE
            WHEN transfers.origin_currency = 'NGN' THEN transfer_quotes.origin_amount_before_fees / origin_parallel.rate
            WHEN transfers.origin_currency != 'NGN' THEN origin_amount_before_fees_in_usd
            ELSE NULL
        END AS origin_amount_before_fees_in_usd_parallel,
        transfers.destination_amount,
        transfers.destination_amount_in_USD, 
        transfer_quotes.destination_amount_before_fees,
        transfer_quotes.destination_amount_before_fees * transfers.destination_rate as destination_amount_before_fees_in_usd,
        transfers.transfer_type,
        transfers.origin_rate,
        transfers.destination_rate,
        destination_parallel.rate AS destination_ngn_parallel_rate,
        CASE
            WHEN transfers.destination_currency = 'NGN' THEN transfers.destination_amount / destination_parallel.rate
            WHEN transfers.destination_currency != 'NGN' THEN transfers.destination_amount_in_USD
            ELSE NULL
        END AS destination_amount_in_USD_parallel,
        CASE
            WHEN transfers.destination_currency = 'NGN' THEN transfer_quotes.destination_amount_before_fees / destination_parallel.rate
            WHEN transfers.destination_currency != 'NGN' THEN destination_amount_before_fees_in_usd
            ELSE NULL
        END AS destination_amount_before_fees_in_usd_parallel,
        incoming_users.primary_currency AS incoming_user_primary_currency,
        outgoing_users.primary_currency AS outgoing_user_primary_currency,

        transfers.journal_type, 
        transfers.corridor, 
        transfers.hlo_table, 
        transfers.hlo_status, 
        transfers.transfer_status, 
        transfers.exchange_rate, 
        CONCAT(transfers.origin_currency,'-',transfers.destination_currency) AS origin_destination_currency_pair, 
        CASE 
            WHEN settlement_rates.settlement_rate IS NOT NULL THEN settlement_rates.date
            WHEN settlement_rates.settlement_rate IS NULL THEN 
                LAST_VALUE(settlement_rates.date) IGNORE NULLS OVER 
		       (PARTITION BY transfers.origin_currency, transfers.destination_currency
		        ORDER BY cast(transfers.hlo_created_at as date)
		        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		    ELSE NULL
		END as settlement_rate_date,

        CASE 
            WHEN settlement_rates.settlement_rate IS NOT NULL THEN settlement_rates.settlement_rate
            WHEN settlement_rates.settlement_rate IS NULL THEN 
                LAST_VALUE(settlement_rates.settlement_rate) IGNORE NULLS OVER 
		       (PARTITION BY transfers.origin_currency, transfers.destination_currency
		        ORDER BY cast(transfers.hlo_created_at as date)
		        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		    ELSE NULL
		END as settlement_rate
        

    FROM chipper.dbt_transformations.expanded_transfers AS transfers
    LEFT JOIN chipper.dbt_transformations.expanded_users AS incoming_users
        ON incoming_users.user_id = transfers.incoming_user_id
    LEFT JOIN chipper.dbt_transformations.expanded_users AS outgoing_users
        ON outgoing_users.user_id = transfers.outgoing_user_id
    LEFT JOIN parallel_rates AS origin_parallel
        ON transfers.origin_currency = origin_parallel.currency
            AND cast(transfers.hlo_created_at AS DATE) = cast(origin_parallel.date AS DATE)
    LEFT JOIN parallel_rates AS destination_parallel
        ON transfers.destination_currency = destination_parallel.currency
            AND cast(transfers.hlo_created_at AS DATE) = cast(destination_parallel.date AS DATE)
    LEFT JOIN chipper.{{ var("core_public") }}.transfer_quotes AS transfer_quotes
        ON transfers.transfer_id = transfer_quotes.transfer_id
    
    LEFT JOIN settlement_rates AS settlement_rates
        ON transfers.origin_currency = settlement_rates.origin_currency
        AND transfers.destination_currency = settlement_rates.destination_currency
        AND cast(transfers.hlo_created_at AS DATE) = cast(settlement_rates.date AS DATE)
),

final AS (

    SELECT
        revenue.*,
        transfers.transfer_quote_transfer_id,
        transfers.origin_currency,
        transfers.destination_currency,
        transfers.outgoing_user_id,
        transfers.incoming_user_id,
        transfers.incoming_user_primary_currency,
        transfers.outgoing_user_primary_currency,
        transfers.exchange_rate_fee_percentage,
        transfers.base_modification_percentage,
        transfers.origin_amount,
        transfers.origin_amount_in_USD,
        transfers.origin_amount_in_USD_parallel,
        transfers.origin_amount_before_fees,
        transfers.origin_amount_before_fees_in_usd,
        transfers.origin_amount_before_fees_in_usd_parallel,
        case 
            when transfers.transfer_quote_transfer_id is not null
            then transfers.origin_amount_before_fees_in_usd_parallel
            else origin_amount_in_USD_parallel
        end as origin_amount_with_transfer_quotes_in_usd_parallel,
        case
            when transfers.transfer_quote_transfer_id is not null
            then transfers.destination_amount_before_fees_in_usd_parallel
            else destination_amount_in_USD_parallel
        end as destination_amount_with_transfer_quotes_in_usd_parallel,
        transfers.destination_amount,
        transfers.destination_amount_in_USD, 
        transfers.destination_amount_in_USD_parallel,
        transfers.destination_amount_before_fees,
        transfers.destination_amount_before_fees_in_usd,
        transfers.destination_amount_before_fees_in_usd_parallel,
        transfers.origin_rate,
        transfers.destination_rate,
        transfers.destination_ngn_parallel_rate,
        ngn_parallel.rate AS ngn_parallel_rate,
        CASE
            WHEN revenue.revenue_stream IN ('FOREX_FEES','CRYPTO_SALES') THEN origin_amount_with_transfer_quotes_in_usd_parallel - destination_amount_with_transfer_quotes_in_usd_parallel
            WHEN revenue.revenue_currency = 'NGN' AND revenue.revenue_stream NOT IN ('FOREX_FEES','CRYPTO_SALES') THEN revenue.gross_revenues / ngn_parallel.rate
            WHEN revenue.revenue_currency != 'NGN' AND revenue.revenue_stream NOT IN ('FOREX_FEES','CRYPTO_SALES') THEN revenue.gross_revenues_in_usd 
            ELSE NULL
        END AS gross_revenues_in_usd_parallel,
        CASE
            WHEN revenue.revenue_currency = 'NGN' THEN revenue.sales_discount / ngn_parallel.rate
            WHEN revenue.revenue_currency != 'NGN' THEN revenue.sales_discount_in_usd
        END AS sales_discount_in_usd_parallel,
        CASE
            WHEN revenue.revenue_stream IN ('FOREX_FEES','CRYPTO_SALES') THEN origin_amount_with_transfer_quotes_in_usd_parallel - destination_amount_with_transfer_quotes_in_usd_parallel
            WHEN revenue.revenue_currency = 'NGN' AND revenue.revenue_stream NOT IN ('FOREX_FEES','CRYPTO_SALES') THEN revenue.net_revenues / ngn_parallel.rate
            WHEN revenue.revenue_currency != 'NGN' AND revenue.revenue_stream NOT IN ('FOREX_FEES','CRYPTO_SALES') THEN revenue.net_revenues_in_usd
            ELSE NULL
        END AS net_revenues_in_usd_parallel,
        transfers.transfer_type as revenue_transfer_type,

        transfers.journal_type, 
        transfers.corridor, 
        transfers.hlo_table, 
        transfers.hlo_status, 
        transfers.transfer_status, 
        transfers.exchange_rate, 
        transfers.origin_destination_currency_pair, 
        transfers.settlement_rate_date,
        transfers.settlement_rate

    FROM chipper.dbt_transformations.revenues AS revenue
    LEFT JOIN transfer_info AS transfers
        ON revenue.transfer_id = transfers.transfer_id
    LEFT JOIN parallel_rates AS ngn_parallel
        ON revenue.revenue_currency = ngn_parallel.currency
            AND cast(revenue.transaction_created_at AS DATE) = cast(ngn_parallel.date AS DATE)
)

SELECT *
FROM final


