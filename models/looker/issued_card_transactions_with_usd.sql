{{ config(materialized='table',
          dist='id', 
          schema='looker') }}

WITH fiat_currency AS (

SELECT 
    DISTINCT ID, 
    type 
FROM {{ref('assets')}}
WHERE type = 'FIAT_CURRENCY'

)

,avg_official_rate AS (

SELECT
    currency.type,
    fx_rate.currency,
    CAST(fx_rate.timestamp AS DATE) AS exchange_rate_date,
    AVG(fx_rate.rate) AS avg_rate
FROM chipper.{{ var("core_public") }}.exchange_rates AS fx_rate

JOIN fiat_currency AS currency
ON fx_rate.Currency = currency.ID

GROUP BY 1,2,3

)
, parallel_rate AS (
SELECT 
DISTINCT currency,
date,
rate
FROM chipper.utils.ngn_usd_parallel_market_rates 
ORDER BY 1,2
)

, all_card_transactions AS (
SELECT card_trans.*,
ROW_NUMBER() OVER (PARTITION BY card_trans.provider_transaction_id ORDER BY card_trans.created_at DESC) AS row_num
FROM chipper.{{ var("core_public") }}.issued_card_transactions AS card_trans
)

, card_transactions_without_duplicates AS (
-- Remove duplicates: when provider transaction ID is not null, for the same provider transaction ID keep 1 internal transaction (row_num = 1); when provider transaction ID is null, keep all transactions
-- Example: card_id = '559d0c21-3c8e-40bb-9ed9-fec2c6ba9e4a'
SELECT card_trans.*
FROM all_card_transactions AS card_trans
WHERE card_trans.provider_transaction_id IS NULL
OR card_trans.row_num = 1
)

, cashback_details AS (
SELECT 
cashback.id AS cashback_id,
cashback.cashback_transfer_id,
cashback.original_transaction_id,
cashback.original_transaction_occurred_at,
cashback.original_transfer_id,
cashback.user_id,
cashback.type AS cashback_type,
cashback.status AS cashback_status,
cashback.created_at AS cashback_created_at,
cashback.updated_at AS cashback_updated_at,
cashback.note AS cashback_note,
cashback.reward_mode AS cashback_reward_mode,
cashback.currency AS cashback_currency,
cashback.amount AS cashback_amount,
cashback.reward_percentage AS cashback_reward_percentage,

transfer.origin_currency,
transfer.origin_amount,
transfer.origin_rate_id,
transfer.destination_currency,
transfer.destination_amount,
transfer.destination_rate_id,
transfer.status AS cashback_transfer_status,
transfer.created_at AS transfer_created_at,
transfer.updated_at AS transfer_updated_at,

CASE 
    WHEN cashback.cashback_transfer_id IS NOT NULL AND cashback.currency = transfer.origin_currency THEN CAST(origin_rate.timestamp AS DATE)
    WHEN cashback.cashback_transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NOT NULL THEN avg_official_rate.exchange_rate_date
    WHEN cashback.cashback_transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NULL 
      THEN LAST_VALUE(avg_official_rate.exchange_rate_date) IGNORE NULLS 
          OVER (PARTITION BY cashback.currency 
          ORDER BY CAST(cashback.updated_at AS DATE)
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ELSE NULL
END AS cashback_official_exchange_rate_date,

CASE 
    WHEN cashback.cashback_transfer_id IS NOT NULL AND cashback.currency = transfer.origin_currency THEN origin_rate.rate
    WHEN cashback.cashback_transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NOT NULL THEN avg_official_rate.avg_rate
    WHEN cashback.cashback_transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NULL 
      THEN LAST_VALUE(avg_official_rate.avg_rate) IGNORE NULLS 
           OVER (PARTITION BY cashback.currency 
           ORDER BY CAST(cashback.updated_at AS DATE)
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ELSE NULL
END AS cashback_official_rate,

parallel_rate.date AS cashback_parallel_rate_date,
parallel_rate.rate AS cashback_parallel_rate,

cashback.amount * cashback_official_rate AS cashback_amount_in_usd,

CASE 
    WHEN cashback.currency != 'NGN' THEN cashback.amount * cashback_official_rate 
    WHEN cashback.currency = 'NGN' THEN cashback.amount / cashback_parallel_rate
    ELSE NULL
END AS cashback_amount_in_usd_parallel

FROM chipper.{{ var("core_public") }}.cashbacks AS cashback

LEFT JOIN chipper.{{ var("core_public") }}.transfers AS transfer
ON cashback.cashback_transfer_id = transfer.id

LEFT JOIN chipper.{{ var("core_public") }}.exchange_rates AS origin_rate
ON transfer.origin_rate_id = origin_rate.id
    
LEFT JOIN avg_official_rate AS avg_official_rate
ON cashback.currency = avg_official_rate.currency
AND CAST(cashback.updated_at AS DATE) = CAST(avg_official_rate.exchange_rate_date AS DATE)

LEFT JOIN parallel_rate AS parallel_rate
ON cashback.currency = parallel_rate.currency
AND CAST(cashback.updated_at AS DATE) = CAST(parallel_rate.date AS DATE)

WHERE cashback.type = 'CARD_TRANSACTION'
)
, final_issued_card_transactions AS (
SELECT 
card_trans.id,
card_trans.journal_id,
card_trans.transfer_id,
card_trans.reverse_transfer_id,
card_trans.user_id,
card_trans.timestamp,
card_trans.created_at,
card_trans.updated_at,
card_trans.type,
card_trans.status,
card_trans.entry_type,
card_trans.currency,
card_trans.amount,
card_trans.fee_currency,
card_trans.fee_amount,
card_trans.error_message,
card_trans.description,
card_trans.card_id,
card_trans.card_last_four,
card_trans.country,
card_trans.provider_card_id,
card_trans.provider_details,
card_trans.provider_transaction_id,

COALESCE(card_trans.provider_details:MccCode::text,
         card_trans.provider_details:Body:BaseResponse:ResponseData:MccCode
         ) AS GTP_MccCode,

COALESCE(card_trans.provider_details:BaseIIStatus::text, 
         card_trans.provider_details:Body:BaseResponse:ResponseData:BaseIIStatus::text,
         card_trans.provider_details:Header:ResponseHeader:StatusMessage::text 
        ) AS base_ii_status,
CASE 
    WHEN (card_trans.type = 'TRANSACTION' AND base_ii_status = 'C') THEN 'CLEARED'
    WHEN (card_trans.type = 'TRANSACTION' AND (base_ii_status = 'R' OR base_ii_status = 'X')) THEN 'REVERSED'
    WHEN (card_trans.type = 'TRANSACTION' AND base_ii_status = '') THEN 'PENDING'
    WHEN (card_trans.type = 'TRANSACTION' AND base_ii_status IS NULL) THEN 'NULL_STATUS'
END AS base_ii_status_definition,

CASE WHEN card_trans.type in ('FUNDING', 'WITHDRAWAL') THEN card_trans.type
     WHEN card_trans.type = 'TRANSACTION' THEN CONCAT(COALESCE(card_trans.entry_type, 'NULL_ENTRY_TYPE'), '_', card_trans.type, '_', base_ii_status_definition)
END AS transaction_type,

card.country AS card_country,
card.currency AS card_currency,
card.card_issuer,

transfer.status AS transfer_status,

CASE 
    WHEN card_trans.provider_transaction_id = 'tx_22797237518eb240a493983cc2c8dc2a26' THEN  card_trans.created_at /* wrong timestamp (1970) for this provider transaction id, transaction currency is USD */
    WHEN card_trans.transfer_id IS NOT NULL AND card_trans.currency = transfer.origin_currency THEN CAST(origin_rate.timestamp AS DATE)
    WHEN card_trans.transfer_id IS NOT NULL AND card_trans.currency = transfer.destination_currency THEN CAST(destination_rate.timestamp AS DATE)
    WHEN card_trans.transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NOT NULL THEN avg_official_rate.exchange_rate_date
    WHEN card_trans.transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NULL 
    THEN LAST_VALUE(avg_official_rate.exchange_rate_date) IGNORE NULLS 
        OVER (PARTITION BY card_trans.currency, card_trans.type
        ORDER BY CAST(card_trans.timestamp AS DATE)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ELSE NULL
END AS official_exchange_rate_date,

CASE 
    WHEN card_trans.provider_transaction_id = 'tx_22797237518eb240a493983cc2c8dc2a26' THEN  1 /* wrong timestamp (1970) for this provider transaction id, transaction currency is USD */
    WHEN card_trans.transfer_id IS NOT NULL AND card_trans.currency = transfer.origin_currency THEN origin_rate.rate
    WHEN card_trans.transfer_id IS NOT NULL AND card_trans.currency = transfer.destination_currency THEN destination_rate.rate
    WHEN card_trans.transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NOT NULL THEN avg_official_rate.avg_rate
    WHEN card_trans.transfer_id IS NULL AND avg_official_rate.exchange_rate_date IS NULL 
    THEN LAST_VALUE(avg_official_rate.avg_rate) IGNORE NULLS 
        OVER (PARTITION BY card_trans.currency,  card_trans.type
        ORDER BY CAST(card_trans.timestamp AS DATE)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ELSE NULL
END AS official_rate,

parallel_rate.date AS parallel_rate_date,
parallel_rate.rate AS parallel_rate,

card_trans.amount * official_rate AS amount_in_usd,

CASE 
    WHEN card_trans.currency != 'NGN' THEN card_trans.amount * official_rate 
    WHEN card_trans.currency = 'NGN' THEN card_trans.amount / parallel_rate
    ELSE NULL
END AS amount_in_usd_parallel,

card_trans.fee_amount * official_rate AS fee_amount_in_usd,

CASE 
    WHEN card_trans.currency != 'NGN' THEN card_trans.fee_amount * official_rate 
    WHEN card_trans.currency = 'NGN' THEN card_trans.fee_amount / parallel_rate
    ELSE NULL
END AS fee_amount_in_usd_parallel,

cashback.cashback_id,
cashback.cashback_transfer_id,
cashback.cashback_type,
cashback.cashback_status,
cashback.cashback_created_at,
cashback.cashback_updated_at,
cashback.cashback_note,
cashback.cashback_reward_mode,
cashback.cashback_currency,
cashback.cashback_amount,
cashback.cashback_reward_percentage,
cashback.cashback_official_exchange_rate_date,
cashback.cashback_official_rate,
cashback.cashback_parallel_rate_date,
cashback.cashback_parallel_rate,
cashback.cashback_amount_in_usd,
cashback.cashback_amount_in_usd_parallel,
cashback.cashback_transfer_status

FROM card_transactions_without_duplicates AS card_trans

LEFT JOIN chipper.{{ var("core_public") }}.issued_cards AS card 
ON card_trans.card_id = card.id
AND card_trans.provider_card_id = card.provider_card_id

LEFT JOIN chipper.{{ var("core_public") }}.transfers AS transfer
ON card_trans.transfer_id = transfer.id

LEFT JOIN cashback_details AS cashback
ON card_trans.id = cashback.original_transaction_id

LEFT JOIN chipper.{{ var("core_public") }}.exchange_rates AS origin_rate
ON transfer.origin_rate_id = origin_rate.id
    
LEFT JOIN chipper.{{ var("core_public") }}.exchange_rates AS destination_rate
ON transfer.destination_rate_id = destination_rate.id 

LEFT JOIN avg_official_rate AS avg_official_rate
ON card_trans.currency = avg_official_rate.currency
AND CAST(card_trans.timestamp AS DATE) = CAST(avg_official_rate.exchange_rate_date AS DATE)

LEFT JOIN parallel_rate AS parallel_rate
ON card_trans.currency = parallel_rate.currency
AND CAST(card_trans.timestamp AS DATE) = CAST(parallel_rate.date AS DATE)
)

SELECT *
FROM final_issued_card_transactions 





