{{ config(materialized='table',
          dist='ledger_entry_id', 
          schema='looker') }}


WITH chargeback_details AS (

SELECT 
    id,
    transfer_id,
    status,
    updated_status,
    amount,
    amount_in_usd,
    chargeback_created_at

FROM chipper.dbt_transformations.expanded_chargebacks as chargebacks
QUALIFY ROW_NUMBER() OVER (PARTITION BY transfer_id ORDER BY transfer_id, chargeback_created_at DESC) =1
),
trans_details as (
SELECT 
    transactions.transfer_id,
    transactions.external_provider,
    transactions.external_provider_transaction_id,    
    transactions.hlo_table,  
    transactions.transaction_details,

    transactions.transaction_details:_internalTransactionDetails:originalTransferForReverseTransferId::text AS reverse_transfer_id,

    transactions.transaction_details:_internalTransactionDetails:commissionPercentage::float AS airtime_commission_percentage,
    transactions.transaction_details:_internalTransactionDetails:discountPercentage::float AS airtime_discount_percentage, 
    transactions.transaction_details:_internalTransactionDetails:phoneCarrier::text AS airtime_phone_carrier,
   
    transactions.transaction_details:externalProviderTransactionDetails:settled:transfer:reference::varchar AS withdrawal_reference_id,
    transactions.transaction_details:_internalTransactionDetails:paymentMethodDetails:type::text AS payment_method_type,
    transactions.transaction_details:_internalTransactionDetails:paymentMethodDetails:isExternal::text AS is_payment_method_external,
    transactions.transaction_details:_internalTransactionDetails:paymentMethodDetails:isLinked::text AS is_payment_method_linked,
    transactions.transaction_details:_internalTransactionDetails:paymentMethodDetails:isVerified::text AS is_payment_method_verified,
    CASE
        WHEN transactions.transaction_details:_internalTransactionDetails:paymentMethodDetails:mobileMoneyPaymentMethodDetails[0]:mobileMoneyCurrency::text is null
            THEN transactions.transaction_details:_internalTransactionDetails:paymentMethodDetails:bankAccountPaymentMethodDetails[0]:bankCurrency::text
        ELSE transactions.transaction_details:_internalTransactionDetails:paymentMethodDetails:mobileMoneyPaymentMethodDetails[0]:mobileMoneyCurrency::text
    END AS cashout_currency
       
FROM chipper.dbt_transformations.transaction_details AS transactions
),
final AS (
    SELECT  
        ledger.ledger_entry_id,
        ledger.transfer_id,
        ledger.is_original_transfer_reversed,
        ledger.is_transfer_reversal,
        ledger.transfer_type,
        ledger.journal_id,
        ledger.journal_type,
        ledger.hlo_id,
        ledger.hlo_table,
        ledger.hlo_status,
        ledger.corridor,
        ledger.hlo_created_at,
        ledger.hlo_updated_at,
        ledger.ledger_currency,
        ledger.ledger_amount,
        ledger.ledger_rate,
        ledger.ledger_amount_in_usd,
        ledger.ledger_timestamp,
        ledger.main_party_user_id,
        ledger.counter_party_user_id,

        case 
            when ledger.ledger_amount < 0 
            then transfers.destination_currency
            else transfers.origin_currency
        end as other_side_currency,

        concat(ledger_currency,'-',other_side_currency) as currency_flow,

        transfers.origin_currency,
        transfers.destination_currency,
        concat(transfers.origin_currency,'-',transfers.destination_currency) as origin_destination_currency_pair,

        ngn_parallel.rate AS ngn_parallel_rate,
        CASE 
            WHEN ledger.ledger_currency != 'NGN' THEN ledger.ledger_amount_in_usd
            WHEN ledger.ledger_currency = 'NGN' AND  ngn_parallel.rate IS NOT NULL THEN ledger.ledger_amount / ngn_parallel.rate
            ELSE NULL
        END ledger_amount_in_usd_parallel,     

        main_user.primary_currency AS main_party_primary_currency,
        main_user.kyc_tier AS main_party_kyc_tier,
        main_user.acquisition_date AS main_party_acquisition_date,
        main_user.acquisition_source AS main_party_acquisition_source,

        main_user.is_internal AS main_party_is_internal,
        main_user.is_deleted AS main_party_is_deleted,
        main_user.is_admin AS main_party_is_admin,
        main_user.is_business AS main_party_is_business,
        main_user.is_valid_user AS main_party_is_valid_user,
        main_user.is_blocked_by_flag AS main_party_is_blocked_by_flag,
        main_user.has_risk_flag AS main_party_has_risk_flag,

        main_user.gender AS main_party_gender,
        main_user.phone_number AS main_party_phone_number,
        main_user.region_latest as main_party_region_latest,

        counter_user.primary_currency AS counter_party_primary_currency,
        counter_user.kyc_tier AS counter_party_kyc_tier,
        counter_user.acquisition_date AS counter_party_acquisition_date,
        counter_user.acquisition_source AS counter_party_acquisition_source,

        counter_user.is_internal AS counter_party_is_internal,
        counter_user.is_deleted AS counter_party_is_deleted,
        counter_user.is_admin AS counter_party_is_admin,
        counter_user.is_business AS counter_party_is_business,
        counter_user.is_valid_user AS counter_party_is_valid_user,
        counter_user.is_blocked_by_flag AS counter_party_is_blocked_by_flag,
        counter_user.has_risk_flag AS counter_party_has_risk_flag,
        

        CASE WHEN chargeback.transfer_id IS NOT NULL THEN 'TRUE' ELSE 'FALSE' END AS is_chargeback,

        transactions.transaction_details,
        transactions.external_provider,
        transactions.external_provider_transaction_id,
  
        transactions.reverse_transfer_id,

        transactions.airtime_commission_percentage,
        transactions.airtime_discount_percentage, 
        transactions.airtime_phone_carrier,

        transactions.withdrawal_reference_id,
        transactions.payment_method_type,
        transactions.is_payment_method_external,
        transactions.is_payment_method_linked,
        transactions.is_payment_method_verified,
        transactions.cashout_currency
       
FROM chipper.dbt_transformations.expanded_ledgers as ledger

LEFT JOIN chipper.dbt_transformations.expanded_transfers as transfers
ON ledger.transfer_id = transfers.transfer_id

JOIN chipper.dbt_transformations.user_demographic_features AS main_user
ON main_user.user_id= ledger.main_party_user_id
AND main_user.is_internal = 'FALSE'
AND main_user.is_deleted = 'FALSE'

LEFT JOIN chipper.dbt_transformations.user_demographic_features AS counter_user 
ON counter_user.user_id =ledger.counter_party_user_id 

LEFT JOIN trans_details AS transactions  
ON transactions.transfer_id = ledger.transfer_id

LEFT JOIN chipper.utils.ngn_usd_parallel_market_rates AS ngn_parallel
ON ledger.ledger_currency = ngn_parallel.currency
AND cast(ledger.ledger_timestamp as DATE) = cast(ngn_parallel.date as DATE)

LEFT JOIN chargeback_details AS chargeback 
ON chargeback.transfer_id =ledger.transfer_id


WHERE ledger.main_party_user_id NOT LIKE 'base%'
  AND ledger.main_party_user_id NOT LIKE 'bot%'
  AND ledger.main_party_user_id NOT LIKE 'issuer%'
  AND ledger.main_party_user_id NOT LIKE 'chipper%'
)

SELECT *
FROM final
