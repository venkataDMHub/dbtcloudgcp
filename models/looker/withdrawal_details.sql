{{ config(materialized='table',
          dist='withdrawal_id', 
          schema='looker') }}

WITH parallel_rates AS (

    SELECT DISTINCT
        date,
        rate,
        currency
    FROM chipper.utils.ngn_usd_parallel_market_rates 
)

,final AS (

SELECT
  withdrawals.id AS withdrawal_id,
  withdrawals.journal_id,
  withdrawals.transfer_id,
  withdrawals.reverse_transfer_id,
  withdrawals.created_at AS withdrawal_created_at,
  withdrawals.updated_at AS withdrawal_updated_at,
  withdrawals.error_message AS withdrawal_error_message, 
  withdrawals.user_id,
  withdrawals.admin_id,
  withdrawals.provider, 
  withdrawals.provider_id, 
  withdrawals.network_id, 
  withdrawals.linked_account_id,
  withdrawals.details AS withdrawal_details, 
  withdrawals.status AS withdrawal_status,
  withdrawals.note,
  withdrawals.is_s2nc,

  users.primary_currency AS user_primary_currency, 
  users.kyc_tier AS user_kyc_tier,
  users.acquisition_date AS user_acquisition_date,
  users.acquisition_source AS user_acquisition_source,
  users.is_internal AS user_is_internal,
  users.is_deleted AS user_is_deleted,
  users.is_admin AS user_is_admin,
  users.is_business AS user_is_business,
  users.is_valid_user AS user_is_valid_user,
  users.is_blocked_by_flag AS user_is_blocked_by_flag,
  users.has_risk_flag AS user_has_risk_flag,

  transaction_details:_internalTransactionDetails:paymentMethodDetails:isExternal::text AS is_payment_method_external,
  transaction_details.external_provider as external_provider,

  transfers.status AS transfer_status,
  transfers.corridor, 
  transfers.exchange_rate,
  transfers.exchange_rate_fee_percentage,

  transfers.origin_rate,
  transfers.origin_currency AS withdrawal_currency, 
  transfers.origin_amount AS withdrawal_amount,
  transfers.origin_amount_in_usd AS withdrawal_amount_in_usd,
  
  origin_ngn_parallel.rate AS origin_ngn_parallel_rate, 
  IFF(transfers.origin_currency != 'NGN', transfers.origin_amount_in_usd, transfers.origin_amount / origin_ngn_parallel.rate) AS withdrawal_amount_in_usd_parallel,

  transfers.destination_rate,
  transfers.destination_currency, 
  transfers.destination_amount,
  transfers.destination_amount_in_usd,
  
  destination_ngn_parallel.rate AS destination_ngn_parallel_rate, 
  IFF(transfers.destination_currency != 'NGN', transfers.destination_amount_in_usd, transfers.destination_amount / destination_ngn_parallel.rate) AS destination_amount_in_usd_parallel


FROM chipper.{{ var("core_public") }}.withdrawals AS withdrawals 

LEFT JOIN {{ref('user_demographic_features')}} AS users
ON withdrawals.user_id = users.user_id
  
LEFT JOIN {{ ref("transfers_with_usd") }} AS transfers
ON withdrawals.transfer_id = transfers.id 
  
LEFT JOIN parallel_rates AS origin_ngn_parallel
ON transfers.origin_currency = origin_ngn_parallel.currency
AND CAST(withdrawals.created_at AS DATE) = CAST(origin_ngn_parallel.date AS DATE)
    
LEFT JOIN parallel_rates AS destination_ngn_parallel
ON transfers.destination_currency = destination_ngn_parallel.currency
AND CAST(withdrawals.created_at AS DATE) = CAST(destination_ngn_parallel.date AS DATE)

LEFT JOIN {{ref('transaction_details')}} AS transaction_details
ON withdrawals.transfer_id = transaction_details.transfer_id
)

SELECT *
FROM final



