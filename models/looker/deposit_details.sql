{{ config(materialized='table',
          dist='deposit_id', 
          schema='looker') }}

{%
  set hardcoded_exchange_rates = { 
    'GHS': 0.19,
    'NGN': 0.0027,
    'KES': 0.0099,
    'UGX': 0.00027,
    'RWF': 0.0011,
    'TZS': 0.00043,
    'ZAR': 0.065 } %}

with parallel_rates AS (

    SELECT DISTINCT
        date,
        rate,
        currency
    FROM chipper.utils.ngn_usd_parallel_market_rates 

),

debit_card_trans_details as (
    select 
        *,
        flattened_payment_method_details.value:paymentCardAuthTokenIssuedBy::text as debit_card_deposit_provider
    from {{ref('transaction_details')}} ,
    lateral flatten (
        input => transaction_details.transaction_details:_internalTransactionDetails:paymentMethodDetails:paymentCardPaymentMethodDetails
    ) as flattened_payment_method_details
    where hlo_table = 'DEPOSITS'
),

debit_card_deposit_providers as (
    select
        transfer_id,
        listagg(debit_card_deposit_provider, ', ') within group (order by transfer_id) as debit_card_deposit_providers
    from debit_card_trans_details
    group by transfer_id

),

deposits as (

SELECT
  deposits.id AS deposit_id,
  deposits.user_id AS user_id,
  deposits.linked_account_id AS linked_account_id,
  deposits.details AS deposit_details,
  deposits.status AS deposit_status,
  deposits.transfer_id AS transfer_id,
  deposits.journal_id AS journal_id,
  deposits.created_at AS deposit_created_at,
  deposits.updated_at AS deposit_updated_at,
  deposits.error_message AS deposit_error_message,
  deposits.charge_id AS charge_id,
  deposits.card_charge_id AS card_charge_id,
  deposits.admin_id AS admin_id,
  deposits.note AS deposit_note,
  deposits.deposit_receipt_webhook_id AS deposit_receipt_webhook_id,
  
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

  CASE  
     WHEN trans_details.transaction_details:_internalTransactionDetails:chargeDetails::text != 'NULL' THEN 'Mobile Money Deposit'
     WHEN trans_details.transaction_details:_internalTransactionDetails:depositWebhooksReceiptDetails::text != 'NULL' THEN 'Nuban Deposit'
     WHEN trans_details.transaction_details:_internalTransactionDetails:cardChargeDetails::text != 'NULL' THEN 'Debit Card Deposit'
     WHEN trans_details.transaction_details:_internalTransactionDetails:bankChargeDetails::text != 'NULL'  THEN 'Bank Account Deposit'
     ELSE 'Other Deposit'
  END AS deposit_type,
  
  CASE
        {% for currency, rate in hardcoded_exchange_rates.items() %}
		       WHEN transfers.origin_rate_id IS NULL
								AND transfers.origin_currency = '{{currency}}' THEN {{ rate }}
				{% endfor %}
				ELSE exchange_rates.rate
	END AS official_rate,

   CASE 
    WHEN deposit_type = 'Debit Card Deposit' THEN debit_card_deposit_providers
    ELSE trans_details.external_provider
  END AS deposit_type_provider,

  transfers.status AS transfer_status,
  transfers.origin_currency AS deposit_currency, 
  transfers.origin_amount AS deposit_amount,
  transfers.origin_amount * exchange_rates.rate AS deposit_amount_in_usd,
  
  ngn_parallel.rate AS ngn_parallel_rate,

  CASE 
    WHEN transfers.origin_currency = 'NGN' THEN transfers.origin_amount / ngn_parallel.rate 
    WHEN transfers.origin_currency != 'NGN' THEN transfers.origin_amount * official_rate
    ELSE NULL
  END AS deposit_amount_in_usd_parallel

FROM chipper.{{var("core_public")}}.deposits as deposits

LEFT JOIN {{ref('user_demographic_features')}} as users
ON deposits.user_id = users.user_id

LEFT JOIN chipper.{{var("core_public")}}.transfers  as transfers
ON deposits.transfer_id = transfers.id

LEFT JOIN chipper.{{var("core_public")}}.exchange_rates as exchange_rates
ON transfers.origin_rate_id = exchange_rates.id

LEFT JOIN {{ref('transaction_details')}} AS trans_details 
ON deposits.transfer_id = trans_details.transfer_id

LEFT JOIN debit_card_deposit_providers 
ON deposits.transfer_id = debit_card_deposit_providers.transfer_id

LEFT JOIN parallel_rates AS ngn_parallel
ON transfers.origin_currency = ngn_parallel.currency
AND CAST(deposits.created_at AS DATE) = CAST(ngn_parallel.date AS DATE)

)

select * from deposits
