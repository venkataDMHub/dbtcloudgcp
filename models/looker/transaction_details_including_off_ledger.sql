{{ config(materialized='table', 
          schema='looker') }}

SELECT
    transaction_details.transfer_id,
    expanded_transfers.is_original_transfer_reversed,
    expanded_transfers.is_transfer_reversal,
    expanded_transfers.transfer_type,
    expanded_transfers.journal_id,
    expanded_transfers.journal_type,
    expanded_transfers.hlo_id,
    transaction_details.hlo_table,
    transaction_details.hlo_status,
    expanded_transfers.corridor,
    transaction_details.hlo_created_at,
    transaction_details.hlo_updated_at,
    expanded_transfers.origin_currency,
    expanded_transfers.origin_amount,
    expanded_transfers.origin_rate,
    expanded_transfers.origin_amount_in_usd,
    transaction_details.outgoing_user_id,
    expanded_transfers.exchange_rate_fee_percentage,
    expanded_transfers.base_modification_percentage,
    expanded_transfers.exchange_rate,
    expanded_transfers.destination_currency,
    expanded_transfers.destination_amount,
    expanded_transfers.destination_rate_id,
    expanded_transfers.destination_rate,
    expanded_transfers.destination_amount_in_usd,
    transaction_details.incoming_user_id,
    expanded_transfers.flat_fee_currency,
    expanded_transfers.flat_fee_amount,
    expanded_transfers.flat_fee_rate,
    expanded_transfers.flat_fee_amount_in_usd,
    transaction_details.external_provider,
    transaction_details.is_on_ledger,
    origin_parallel.rate AS ngn_parallel_rate,
    destination_parallel.rate as destination_ngn_parallel_rate,
    outgoing_user.primary_currency AS outgoing_user_primary_currency,
    outgoing_user.kyc_tier AS outgoing_user_kyc_tier,
    outgoing_user.acquisition_date AS outgoing_user_acquisition_date,
    outgoing_user.acquisition_source AS outgoing_user_acquisition_source,
    outgoing_user.is_internal AS outgoing_user_is_internal,
    outgoing_user.is_deleted AS outgoing_user_is_deleted,
    outgoing_user.is_admin AS outgoing_user_is_admin,
    outgoing_user.is_business AS outgoing_user_is_business,
    outgoing_user.is_valid_user AS outgoing_user_is_valid_user,
    outgoing_user.is_blocked_by_flag AS outgoing_user_is_blocked_by_flag,
    outgoing_user.has_risk_flag AS outgoing_user_has_risk_flag,
    outgoing_user.gender AS outgoing_user_gender,
    outgoing_user.phone_number AS outgoing_user_phone_number,
    outgoing_user.region_latest AS outgoing_user_region_latest,
    outgoing_user.country_latest as outgoing_user_country_latest,
    incoming_user.country_latest as incoming_user_country_latest,
    incoming_user.primary_currency AS incoming_user_primary_currency,
    incoming_user.kyc_tier AS incoming_user_kyc_tier,
    incoming_user.acquisition_date AS incoming_user_acquisition_date,
    incoming_user.acquisition_source AS incoming_user_acquisition_source,
    incoming_user.is_internal AS incoming_user_is_internal,
    incoming_user.is_deleted AS incoming_user_is_deleted,
    incoming_user.is_admin AS incoming_user_is_admin,
    incoming_user.is_business AS incoming_user_is_business,
    incoming_user.is_valid_user AS incoming_user_is_valid_user,
    incoming_user.is_blocked_by_flag AS incoming_user_is_blocked_by_flag,
    incoming_user.has_risk_flag AS incoming_user_has_risk_flag,
    transaction_details:externalProviderTransactionDetails:settled:transfer:reference::varchar AS withdrawal_reference_id,
    transaction_details:_internalTransactionDetails:paymentMethodDetails:type::text AS payment_method_type,
    transaction_details:_internalTransactionDetails:paymentMethodDetails:isExternal::text AS is_payment_method_external,
    transaction_details:_internalTransactionDetails:paymentMethodDetails:isLinked::text AS is_payment_method_linked,
    transaction_details:_internalTransactionDetails:paymentMethodDetails:isVerified::text AS is_payment_method_verified,
    CASE
        WHEN
            transaction_details:_internalTransactionDetails:paymentMethodDetails:mobileMoneyPaymentMethodDetails[
                0
            ]:mobileMoneyCurrency::text IS NULL
            THEN transaction_details:_internalTransactionDetails:paymentMethodDetails:bankAccountPaymentMethodDetails[
                0
            ]:bankCurrency::text
        ELSE
            transaction_details:_internalTransactionDetails:paymentMethodDetails:mobileMoneyPaymentMethodDetails[
                0
            ]:mobileMoneyCurrency::text
    END AS cashout_currency,
    CASE
        WHEN
            expanded_transfers.origin_currency != 'NGN' THEN expanded_transfers.origin_amount_in_usd
        WHEN
            expanded_transfers.origin_currency = 'NGN' AND origin_parallel.rate IS NOT NULL THEN expanded_transfers.origin_amount / origin_parallel.rate
    END AS origin_amount_in_usd_parallel,
    CASE
        WHEN
            expanded_transfers.destination_currency != 'NGN' THEN expanded_transfers.destination_amount_in_usd
        WHEN
            expanded_transfers.destination_currency = 'NGN' AND destination_parallel.rate IS NOT NULL THEN expanded_transfers.destination_amount / destination_parallel.rate
    END AS destination_amount_in_usd_parallel
FROM {{ ref('transaction_details') }} AS transaction_details

LEFT JOIN {{ ref('expanded_transfers') }} AS expanded_transfers
    ON expanded_transfers.transfer_id = transaction_details.transfer_id

LEFT JOIN {{ ref('user_demographic_features') }} AS outgoing_user
    ON outgoing_user.user_id = transaction_details.outgoing_user_id

LEFT JOIN {{ ref('user_demographic_features') }} AS incoming_user
    ON incoming_user.user_id = transaction_details.incoming_user_id

LEFT JOIN chipper.utils.ngn_usd_parallel_market_rates AS origin_parallel
    ON origin_parallel.currency = expanded_transfers.origin_currency
        AND cast(expanded_transfers.hlo_created_at AS date) = cast(origin_parallel.date AS date)

LEFT JOIN chipper.utils.ngn_usd_parallel_market_rates AS destination_parallel
    ON destination_parallel.currency = expanded_transfers.destination_currency
        AND cast(expanded_transfers.hlo_created_at AS date) = cast(destination_parallel.date AS date)
