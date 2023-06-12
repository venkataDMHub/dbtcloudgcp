{{  config(
        materialized='incremental',
        unique_key='ledger_entry_id',
        on_schema_change='append_new_columns') }}

with address as (

SELECT
    addresses.user_id,
    addresses.created_at,
    addresses.house_number,
    addresses.street,
    addresses.city,
    addresses.region,
    addresses.postal_code,
    addresses.country,

    CASE WHEN addresses.country = 'US' 
        THEN COALESCE(TRIM(CONCAT(addresses.house_number,' ', addresses.street,' ', addresses.city,' ',addresses.region,' ', addresses.postal_code)),'UNK')
        ELSE COALESCE(TRIM(CONCAT(addresses.house_number,' ', addresses.street,' ', addresses.city,' ',addresses.country,' ', addresses.postal_code)),'UNK')
    END as address
  
    FROM "CHIPPER".{{var("compliance_public")}}."ADDRESSES" as addresses
    QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY CREATED_AT DESC) = '1'

),

withdrawals as (

SELECT

    ledgers.ledger_entry_id,

    REPLACE(COALESCE(TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails:type::string,''),'_',' ') as withdrawal_type,

    CONCAT(COALESCE(TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.bankAccountPaymentMethodDetails[0].bankName::string,   
    TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.mobileMoneyPaymentMethodDetails[0].mobileMoneyCarrier::string,
    TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.paymentCardPaymentMethodDetails[0].paymentCardIssuingBank::string),
    ' ',
    COALESCE(RIGHT(TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.bankAccountPaymentMethodDetails[0].bankAccountNumber::string,4), 
    TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.mobileMoneyPaymentMethodDetails[0].mobileMoneyPhone::string,
    TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.paymentCardPaymentMethodDetails[0].paymentCardLastFour::string)) AS acct_details

    FROM {{ref('expanded_ledgers')}} as ledgers

    LEFT JOIN {{ref('transaction_details')}} as details

    ON ledgers.transfer_id = details.transfer_id
    WHERE ledgers.hlo_table = 'WITHDRAWALS'

),

user_statement as (

SELECT

    uuid_string() as record_id,

    ledgers.ledger_entry_id,

    CONCAT(monthname(ledgers.ledger_timestamp),'-',year(ledgers.ledger_timestamp)) AS month_year,

    ledgers.main_party_user_id AS user_id,

    user_info.primary_currency AS user_primary_currency,

    LEFT(user_info.legal_first_name,200) AS first_name,
    LEFT(user_info.legal_last_name,200) AS last_name,
    user_info.tag AS chipper_user_tag,

    addresses.house_number as house_number,
    addresses.street as street_name,
    addresses.city as city,
    addresses.region as region,
    addresses.postal_code as postal_code,
    addresses.country as country,
    LEFT(COALESCE(addresses.address,'UNK'),250) as address,

    ledgers.ledger_timestamp AS transaction_date,
    ledgers.transfer_id AS transfer_id,
    ledgers.ledger_currency AS ledger_currency,
    ledgers.transfer_type AS transfer_type, 
    ledgers.hlo_table AS hlo_table,
    ledgers.is_transfer_reversal AS is_transfer_reversal,
    ledgers.hlo_status AS status,

    LEFT(CASE
        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.ledger_amount > '0' AND ledgers.transfer_type = 'PAYMENTS_P2P_SETTLED')
        THEN COALESCE(CONCAT('RECEIVED MONEY',
                    CHAR(10),
                    'FROM: ',
                    UPPER(counter_user_info.legal_first_name),
                    ' ',
                    UPPER(counter_user_info.legal_last_name)
             ), 'P2P PAYMENT - RECEIVED MONEY')

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.ledger_amount < '0' AND ledgers.transfer_type = 'PAYMENTS_P2P_SETTLED')
        THEN COALESCE(CONCAT('SENT MONEY',
                    CHAR(10),
                    'TO: ',
                    UPPER(counter_user_info.legal_first_name),
                    ' ',
                    UPPER(counter_user_info.legal_last_name)
             ), 'P2P PAYMENT - SENT MONEY')

        WHEN (ledgers.transfer_type = 'REFERRAL_BONUS_SETTLED')
        THEN 'REFERRAL BONUS'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_REFERRAL_BONUS_SETTLED')
        THEN 'REFERRAL BONUS'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_WELCOME_BONUS_SETTLED')
        THEN 'WELCOME BONUS'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_MOBILE_MONEY_CONSUMER_FEE_REIMBURSEMENT_SETTLED')
        THEN 'MOBILE MONEY CONSUMER FEE REIMBURSEMENT'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'WITHDRAWALS_REVERSAL')
        THEN 'FAILED WITHDRAWAL - REFUND'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_BOT_TRANSFERS_SETTLED')
        THEN 'BOT PAYMENT'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'DEPOSITS_SETTLED')
        THEN 'DEPOSIT SETTLED'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_ACTIVATION_TOOLING_PAYOUT_SETTLED')
        THEN 'ACTIVATION TOOLING PAYOUT'

        WHEN (ledgers.transfer_type = 'CASHBACKS_SETTLED')
        THEN 'CASHBACK'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_CASHBACK_SETTLED')
        THEN 'CASHBACK'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'DATA_PURCHASES_REVERSAL')
        THEN 'FAILED DATA PURCHASE'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_WITHDRAWALS_REVERSAL_OTHER_SETTLED')
        THEN 'FAILED WITHDRAWAL - REFUND'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_ACCOUNT_MERGES_SETTLED')
        THEN 'ACCOUNT MERGE PAYMENT'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_PAYMENTS_FROM_BASE_OTHER_SETTLED')
        THEN 'PAYMENT FROM BASE'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_COLLECTIONS_TO_BASE_SETTLED')
        THEN 'BASE COLLECTION - INCIDENT'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'NETWORK_API_B2C_SETTLED')
        THEN 'NETWORK API PAYOUT'

        WHEN (ledgers.hlo_table = 'PAYMENTS' AND ledgers.transfer_type = 'PAYMENTS_DATA_PURCHASES_REVERSAL_OTHER_SETTLED')
        THEN 'FAILED DATA PURCHASE'

        WHEN ledgers.hlo_table = 'DEPOSITS'
        THEN CONCAT('ADDED CASH FROM ',
                    REPLACE(transactions.TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.type::string,'_',' '),
                    ' ',
                    COALESCE(transactions.TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.paymentCardPaymentMethodDetails[0].paymentCardLastFour::string,
                             transactions.TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.mobileMoneyPaymentMethodDetails[0].mobileMoneyPhone::string,
                             transactions.TRANSACTION_DETAILS:_internalTransactionDetails.paymentMethodDetails.bankAccountPaymentMethodDetails[0].bankAccountNumber::string,
                             '')
             )

        WHEN ledgers.hlo_table = 'BILL_PAYMENTS'
        THEN 'BILL PAYMENT - RECHARGE'

        WHEN ledgers.hlo_table = 'WITHDRAWALS'
        THEN CONCAT('SENT MONEY TO ',
                    withdrawals.withdrawal_type,
                    ' ',
                    withdrawals.acct_details
            )

        WHEN ledgers.hlo_table = 'AIRTIME_PURCHASES'
        THEN CONCAT('AIRTIME PURCHASE',
                    CHAR(10),
                    'CARRIER: ',
                    transactions.TRANSACTION_DETAILS:_internalTransactionDetails.phoneCarrier::string,
                    CHAR(10),
                    'PHONE NUMBER: ',
                    transactions.TRANSACTION_DETAILS:_internalTransactionDetails.phoneNumber::string     
             )

        WHEN ledgers.hlo_table = 'REQUESTS'
        THEN 'REQUEST'

        WHEN ledgers.hlo_table = 'STOCK_TRADES'
        THEN REPLACE(ledgers.transfer_type,'_',' ')

        WHEN ledgers.hlo_table = 'CHECKOUTS'
        THEN 'CHECKOUT'

        WHEN ledgers.hlo_table = 'DATA_PURCHASES'
        THEN CONCAT('DATA PURCHASE',
                    CHAR(10),
                    'PHONE NUMBER: ',
                    transactions.TRANSACTION_DETAILS:_internalTransactionDetails.phoneNumber::string
             )

        WHEN ledgers.hlo_table = 'ORDERS'
        THEN REPLACE(ledgers.transfer_type,'_',' ')
        
        WHEN ledgers.hlo_table = 'PAYMENT_INVITATIONS'
        THEN 'PAYMENT INVITATION'
        
        WHEN ledgers.hlo_table = 'ISSUED_CARD_TRANSACTIONS'
        THEN CONCAT('ISSUED CARD TRANSACTION - ',
                   UPPER(transactions.TRANSACTION_DETAILS:_internalTransactionDetails:description::text)
             )

        WHEN ledgers.hlo_table = 'CRYPTO_DEPOSITS'
        THEN CONCAT('CRYPTO DEPOSIT - ',ledgers.ledger_currency)
        
        WHEN ledgers.hlo_table = 'CRYPTO_WITHDRAWALS'
        THEN CONCAT('CRYPTO WITHDRAWAL - ',ledgers.ledger_currency)
        
        WHEN ledgers.transfer_type = 'ASSET_TRADES_BUY_SETTLED'
        THEN CONCAT('ASSET TRADE',
                   CHAR(10),
                   transactions.TRANSACTION_DETAILS:_internalTransactionDetails:position::string,
                   ': ',
                   transactions.TRANSACTION_DETAILS:_internalTransactionDetails:asset::string
             )

        WHEN ledgers.transfer_type = 'ASSET_TRADES_SELL_SETTLED'
        THEN CONCAT('ASSET TRADE',
                   CHAR(10),
                   transactions.TRANSACTION_DETAILS:_internalTransactionDetails:position::string,
                   ': ',
                   transactions.TRANSACTION_DETAILS:_internalTransactionDetails:asset::string
             )

        ELSE REPLACE(ledgers.transfer_type,'_',' ')

    END,200) AS description,

    CASE
        WHEN ledgers.ledger_amount >= 0 THEN ledgers.ledger_amount else '0' 
    END as inflow_local,

    CASE
        WHEN ledgers.ledger_amount < 0 THEN ledgers.ledger_amount else '0' 
    END as outflow_local,

    SUM(ledgers.ledger_amount) OVER (PARTITION BY ledgers.main_party_user_id ORDER BY ledgers.ledger_timestamp) AS user_balance_local,

    CASE
        WHEN ledgers.ledger_amount_in_usd >= 0 THEN ledgers.ledger_amount_in_usd else '0' 
    END as inflow_usd,

    CASE
        WHEN ledgers.ledger_amount_in_usd < 0 THEN ledgers.ledger_amount_in_usd else '0' 
    END as outflow_usd,

    SUM(ledgers.ledger_amount_in_usd) OVER (PARTITION BY ledgers.main_party_user_id ORDER BY ledgers.ledger_timestamp) AS user_balance_usd

FROM {{ref('expanded_ledgers')}} AS ledgers

LEFT JOIN {{ref('expanded_users')}} AS user_info 
    ON ledgers.main_party_user_id = user_info.user_id

LEFT JOIN {{ref('transaction_details')}} AS transactions 
    ON ledgers.transfer_id = transactions.transfer_id

LEFT JOIN {{ref('expanded_users')}} AS counter_user_info 
    ON ledgers.counter_party_user_id = counter_user_info.user_id

LEFT JOIN address as addresses
    ON ledgers.main_party_user_id = addresses.user_id

LEFT JOIN withdrawals as withdrawals
    ON ledgers.ledger_entry_id = withdrawals.ledger_entry_id
 
WHERE user_info.is_internal = 'FALSE'
AND ledger_currency = user_primary_currency

)

SELECT
    record_id,
    ledger_entry_id,
    month_year,
    user_id,
    user_primary_currency,
    first_name,
    last_name,
    chipper_user_tag,
    house_number,
    street_name,
    city,
    region,
    postal_code,
    country,
    address,
    transaction_date,
    transfer_id,
    ledger_currency,
    transfer_type,
    hlo_table,
    is_transfer_reversal,
    status,
    description,

    round(inflow_local,2) as inflow_local,
    round(outflow_local,2) as outflow_local,
    round(user_balance_local,2) as user_balance_local,
    round(inflow_usd,2) as inflow_usd,
    round(outflow_usd,2) as outflow_usd,
    round(user_balance_usd,2) as user_balance_usd

FROM user_statement
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where transaction_date >= (select max(transaction_date) from {{ this }})
{% endif %}

ORDER BY user_id, transaction_date desc

