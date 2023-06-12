{{ config(materialized='table',
          dist='id', 
          schema='looker') }}

WITH 
fiat AS (
    SELECT DISTINCT id, type 
    FROM {{ref('assets')}}
    WHERE type = 'FIAT_CURRENCY'
),

latest_official_rate AS (
SELECT
    fiat.type,
    exchange_rates.currency,
    exchange_rates.timestamp,
    exchange_rates.rate 
    
FROM chipper.{{ var("core_public") }}.exchange_rates AS exchange_rates
JOIN fiat ON exchange_rates.currency = fiat.id

WHERE exchange_rates.currency != 'NONE'

QUALIFY ROW_NUMBER () OVER (PARTITION BY exchange_rates.currency ORDER BY exchange_rates.timestamp DESC) = 1

ORDER BY 1,2
),

latest_parallel_rate as (
SELECT DISTINCT date,
rate,
currency
FROM chipper.utils.ngn_usd_parallel_market_rates AS parallel_rate 
QUALIFY ROW_NUMBER () OVER (PARTITION BY parallel_rate.currency ORDER BY parallel_rate.date DESC) = 1
)


SELECT  issued_cards.id,
        issued_cards.country,
        issued_cards.card_network,
        issued_cards.expiry_date,
        issued_cards.provider_card_status,
        issued_cards.created_at,
        issued_cards.billing_address,
        issued_cards.card_network_logo,
        issued_cards.provider_details,
        issued_cards.card_status,
        issued_cards.card_issuer,
        issued_cards.provider_card_id,
        issued_cards.updated_at,
        issued_cards.balance,
        issued_cards.phone,
        issued_cards.user_id,
        issued_cards.tokenized_cvv,
        issued_cards.last_four,
        issued_cards.card_bank,
        issued_cards.replacement_card_id,
        issued_cards.tokenized_card_number,
        issued_cards.currency,
        issued_cards.name_on_card,
        issued_cards.provider_details:Body:CardStatus::text as parsed_card_status,
        CASE WHEN  issued_cards.balance > 0 THEN TRUE ELSE FALSE END AS has_card_balance,
        latest_official_rate.timestamp AS official_exchange_rate_timestamp,
        latest_official_rate.rate AS official_exchange_rate,
        issued_cards.balance * latest_official_rate.rate AS balance_in_usd,
        latest_parallel_rate.date AS parallel_rate_date,
        latest_parallel_rate.rate AS parallel_rate,
        CASE 
            WHEN issued_cards.currency != 'NGN' THEN issued_cards.balance * latest_official_rate.rate
            WHEN issued_cards.currency = 'NGN' THEN issued_cards.balance / latest_parallel_rate.rate 
            ELSE NULL
            END AS balance_in_usd_parallel

FROM chipper.{{ var("core_public") }}.issued_cards AS issued_cards

LEFT JOIN latest_official_rate
ON issued_cards.currency = latest_official_rate.currency

LEFT JOIN latest_parallel_rate
ON issued_cards.currency = latest_parallel_rate.currency