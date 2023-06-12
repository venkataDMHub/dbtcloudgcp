{{ config(materialized='table', 
          schema='looker') }}

WITH verified_cte AS (

SELECT user_tiers.user_id AS id, 
       COUNT(doc_status_change.document_id) AS tier 
FROM chipper.{{var("compliance_public")}}.document_status_changes AS doc_status_change
LEFT JOIN chipper.{{var("compliance_public")}}.KYC_DOCUMENTS AS kyc_docs ON doc_status_change.document_id=kyc_docs.id
LEFT JOIN chipper.{{var("compliance_public")}}.USER_TIERS AS user_tiers ON  user_tiers.user_id=kyc_docs.owner_id
WHERE ((lower(doc_status_change.reason) like '%flutterwave%')
       AND user_tiers.tier in ({{ verified_tiers() }})
       AND doc_status_change.new_status = 'ACCEPTED')
GROUP BY user_tiers.user_id),


cte_last_txn AS (
    SELECT 
        main_party_user_id, 
        MAX(ledger_timestamp) AS last_transaction_date 
    FROM {{ref('expanded_ledgers')}}
    GROUP BY main_party_user_id),

sequence_tags AS (
        SELECT id,
               first_name, 
               last_name,
               JAROWINKLER_SIMILARITY(tag, LAG(tag) OVER(ORDER BY first_name, last_name)) AS percent_similarity,
               tag, 
               LAG(tag) OVER(ORDER BY first_name, last_name) AS compare,
               percent_similarity > 70 AS similar 
         FROM CHIPPER.{{ var("core_public") }}.USERS
          WHERE tag IS NOT NULL
          ORDER BY first_name ASC, last_name ASC, tag ASC),

stock_balance AS (
SELECT user_id as stock_user,
payload,
payload:equityValue::text AS stock_balance_usd,
updated_at AS most_recent_stock_balance_update_timestamp
FROM chipper.{{ var("core_public") }}.stock_balance_history 
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY most_recent_stock_balance_update_timestamp DESC) = 1
ORDER BY 3 DESC

),
  
user_change_info_dob as 
(select 
       expanded_users.user_id AS user_id,
       expanded_users.acquisition_source,
       user_info.dob AS current_user_dob,
       user_changes.source AS pii_data_source,
       max(user_changes.timestamp) AS most_recent_update_date,
       LEFT(trim(user_changes.DIFF:dob[0]::text, '""[]T::.Znull'),10) AS dob_user_change_info_1,
       LEFT(trim(user_changes.DIFF:dob[1]::text, '""[]T::.Znull'),10) AS dob_user_change_info_2,
       CASE WHEN current_user_dob=dob_user_change_info_1 THEN 'Y' ELSE 'N' END AS current_dob_match_1,
       CASE WHEN dob_user_change_info_2 IS NOT NULL AND current_user_dob=dob_user_change_info_2 THEN 'Y'
            WHEN dob_user_change_info_2 IS NULL THEN NULL
            ELSE 'N' 
            END AS current_dob_match_2
       FROM {{ref('expanded_users')}} AS expanded_users
       LEFT JOIN chipper.{{var("compliance_public")}}.USER_INFO_CHANGES AS user_changes ON user_changes.user_id = expanded_users.user_id
       LEFT JOIN chipper.{{var("compliance_public")}}.USER_INFO AS user_info on expanded_users.user_id=user_info.user_id    
       WHERE dob_user_change_info_2 is not null or dob_user_change_info_1 is not null
       GROUP BY expanded_users.user_id,
                user_info.dob,
                expanded_users.acquisition_source,
                user_changes.source,
                user_changes.DIFF:dob[0]::text,
                user_changes.DIFF:dob[1]::text
          QUALIFY ROW_NUMBER() OVER (PARTITION BY expanded_users.user_id ORDER BY expanded_users.user_id, most_recent_update_date DESC) =1),
                


gibberish AS (
   SELECT user_id,
          lower(legal_first_name) AS user_first_name,
          lower(legal_last_name) AS user_last_name,
          CASE WHEN users.legal_first_name IS NULL THEN 'GIBBERISH' 
            WHEN users.legal_last_name IS NULL THEN 'GIBBERISH'
            WHEN users.legal_first_name = '' THEN 'GIBBERISH'
            WHEN users.legal_last_name = '' THEN 'GIBBERISH'
            WHEN user_last_name NOT LIKE ('%a%') AND user_last_name NOT LIKE ('%e%') AND user_last_name NOT LIKE ('%i%') AND user_last_name NOT LIKE ('%o%') AND user_last_name NOT LIKE ('%u%') 
            AND user_last_name NOT LIKE ('a%') AND user_last_name NOT LIKE ('e%') AND user_last_name NOT LIKE ('i%') AND user_last_name NOT LIKE ('o%') AND user_last_name NOT LIKE ('u%') 
            AND user_last_name NOT LIKE ('%a') AND user_last_name NOT LIKE ('%e') AND user_last_name NOT LIKE ('%i') AND user_last_name NOT LIKE ('%o') AND user_last_name NOT LIKE ('%u') THEN 'GIBBERISH' 
            when user_first_name NOT LIKE ('%a%') AND user_first_name NOT LIKE ('%e%') AND user_first_name NOT LIKE ('%i%') AND user_first_name NOT LIKE ('%o%') AND user_first_name NOT LIKE ('%u%') 
            AND user_first_name NOT LIKE ('a%') AND user_first_name NOT LIKE ('e%') AND user_first_name NOT LIKE ('i%') AND user_first_name NOT LIKE ('o%') AND user_first_name NOT LIKE ('u%')
            AND user_first_name NOT LIKE ('%a') AND user_first_name NOT LIKE ('%e') AND user_first_name NOT LIKE ('%i') AND user_first_name NOT LIKE ('%o') AND user_first_name NOT LIKE ('%u') THEN 'GIBBERISH' 
            WHEN REGEXP_LIKE(user_first_name,  '.*[^a-zA-Z ].*') THEN 'GIBBERISH' 
            WHEN REGEXP_LIKE(user_last_name,  '.*[^a-zA-Z ].*') THEN 'GIBBERISH' 
            WHEN user_first_name=user_last_name THEN 'GIBBERISH' 
            ELSE ' '
            END AS gibberish
        FROM {{ref('expanded_users')}} as users),


latest_fx as (
SELECT currency,
rate,
timestamp
from chipper.{{ var("core_public") }}.exchange_rates as a
join {{ref('assets')}} as b
on a.currency = b.id
and b.type = 'FIAT_CURRENCY'
qualify row_number() over (partition by currency order by timestamp desc) = 1
),

card_balance as ( 
select 
    issued_cards.user_id as card_user_id,
    sum(balance * rate) as balance_in_usd
from chipper.{{ var("core_public") }}.issued_cards 
left join latest_fx 
on issued_cards.currency =  latest_fx.currency
group by 1
  order by balance_in_usd desc
),


final AS (
  SELECT 
       users.user_id AS user_id,
       users.legal_first_name AS user_first_name,
       users.legal_last_name AS user_last_name,
       users.acquisition_date AS account_open_date,
       users.primary_currency AS user_primary_currency,
       users.tag AS user_tag,
       users.kyc_tier AS user_kyc_tier,
       users.is_valid_user AS is_valid_user,
       users.is_blocked_by_flag AS is_blocked_by_flag,
       users.dob AS user_dob,
       users.user_age AS user_age,
       users.city_of_birth AS user_birth_country,
       users.street_first AS user_street,
  
       cte_last_txn.last_transaction_date AS user_last_txn_date,
       
       onboarding_score.is_fake_first_name,
       onboarding_score.is_fake_last_name,
       
       user_change_info_dob.pii_data_source AS pii_data_source,
       user_change_info_dob.dob_user_change_info_1 AS dob_user_change_info_1,
       user_change_info_dob.dob_user_change_info_2 AS dob_user_change_info_2,
       user_change_info_dob.current_dob_match_1 AS user_change_dob_mismatch,
       user_change_info_dob.current_dob_match_2 AS current_dob_match_2,
       
       gibberish.gibberish AS gibberish_flag,
       
	  CASE WHEN user_latest_wallet_balance_usd.total_fiat_balance_usd IS NOT NULL THEN CAST(user_latest_wallet_balance_usd.total_fiat_balance_usd AS INT) ELSE 0 END AS latest_fiat_balance_usd,
       CASE WHEN user_latest_wallet_balance_usd.total_crypto_balance_usd IS NOT NULL THEN CAST(user_latest_wallet_balance_usd.total_crypto_balance_usd AS INT) ELSE 0 END AS latest_crypto_wallet_balance_usd,
       CASE WHEN stock_balance.stock_balance_usd IS NOT NULL THEN CAST(stock_balance.stock_balance_usd AS INT) ELSE 0 END AS latest_stock_balance_usd,
       CASE WHEN card_balance.balance_in_usd IS NOT NULL THEN CAST(card_balance.balance_in_usd AS INT) ELSE 0 END as latest_card_balance_usd,
       (latest_fiat_balance_usd+latest_crypto_wallet_balance_usd+latest_stock_balance_usd+latest_card_balance_usd) AS total_user_balance_usd,
       
       DATEDIFF(month, cte_last_txn.last_transaction_date, getdate()) AS months_since_last_txn,
       
       CASE WHEN months_since_last_txn BETWEEN 0 AND 6 THEN 'ACTIVITY_WITHIN_LAST_6_MO' 
            WHEN months_since_last_txn >6  AND  months_since_last_txn <=12 THEN 'NO_ACTIVITY_IN_6_MO'
            WHEN months_since_last_txn >12 THEN 'NO_ACTIVITY_OVER_12_MO'
            ELSE ' '
            END AS activity_status,
            
       CASE WHEN total_user_balance_usd = 0 THEN 'ZERO_BALANCE'
            WHEN total_user_balance_usd < 0 THEN 'NEGATIVE_BALANCE'
            WHEN total_user_balance_usd > 5 THEN 'BALANCE_>_5'
            WHEN total_user_balance_usd <= 5 THEN 'BALANCE_<=_5'
            ELSE ' '
            END AS balance_status_usd,
            
       CASE WHEN onboarding_score.is_fake_first_name = 'TRUE' or onboarding_score.is_fake_last_name = 'TRUE' THEN 'FAKE_NAME' END AS uos_fake_name_generator,  
       
       CASE WHEN user_age<18 THEN 'Unreal_DOB'
            WHEN user_age>99 THEN 'Unreal_DOB'
            WHEN month(users.dob)>12 THEN 'Unreal_DOB'
            WHEN day(users.dob)>31 THEN 'Unreal_DOB'
            WHEN users.dob is null THEN 'Unreal_DOB' 
            WHEN users.dob = '2001-01-01' THEN 'Unreal_DOB'
            WHEN users.dob LIKE '' THEN 'Unreal_DOB'
            ELSE 'Real_DOB'
            END AS dob_error,
            
       CASE WHEN sequence_tags.similar = 'TRUE' THEN 'SEQUENCED_TAGS' 
       ELSE ' '
       END AS sequenced_tags,
       
       CASE WHEN users.city_of_birth IS null OR users.city_of_birth = '' THEN 'BIRTHPLACE_MISSING'
       ELSE ' '
       END AS blank_info_birthplace,
       
       CASE WHEN users.street_first IS NULL OR users.street_first IN ('  ',' ','   ','    ','     ','      ','       ','') THEN 'ADDRESS_FIELD_MISSING' 
       ELSE ' '
       END AS blank_info_address,
       
       CASE WHEN verified_cte.id is not null THEN 'Y' 
       ELSE 'N'
       END AS has_flutterwave_verification,
       
       users.total_lpv_usd AS total_lpv_usd
       
       FROM {{ref('user_demographic_features')}} as users
       LEFT JOIN {{ref('latest_wallet_balances_usd')}} AS user_latest_wallet_balance_usd ON users.user_id = user_latest_wallet_balance_usd.user_id
       LEFT JOIN "CHIPPER"."UTILS"."USER_ONBOARDING_SCORE" as onboarding_score ON onboarding_score.user_id=users.user_id
       LEFT JOIN verified_cte ON verified_cte.ID = users.user_id
       LEFT JOIN cte_last_txn ON users.user_id =cte_last_txn.main_party_user_id
       LEFT JOIN sequence_tags ON sequence_tags.id = users.user_id
       LEFT JOIN stock_balance ON stock_balance.stock_user=users.user_id
       LEFT JOIN user_change_info_dob ON user_change_info_dob.user_id=users.user_id
       LEFT JOIN gibberish ON gibberish.user_id=users.user_id
       LEFT JOIN card_balance ON card_balance.card_user_id=users.user_id
  
       GROUP BY users.user_id,
                users.legal_first_name,
                users.legal_last_name,
                users.tag,
                users.dob,
                user_age,
                users.kyc_tier,
                users.is_valid_user,
                users.is_blocked_by_flag,
                account_open_date,
                users.primary_currency,
                users.city_of_birth,
                users.street_first,
                onboarding_score.is_fake_first_name,
                onboarding_score.is_fake_last_name,
                users.primary_currency,
                user_latest_wallet_balance_usd.total_fiat_balance_usd,
                user_latest_wallet_balance_usd.total_crypto_balance_usd,
                months_since_last_txn,
                activity_status,           
                balance_status_usd,
                gibberish,
                dob_error,
                blank_info_birthplace,
                blank_info_address,
                has_flutterwave_verification,
                sequenced_tags,
                cte_last_txn.last_transaction_date,
                users.total_lpv_usd,
                total_user_balance_usd,
                latest_stock_balance_usd,
                user_change_info_dob.pii_data_source,
                user_change_info_dob.dob_user_change_info_1,
                user_change_info_dob.dob_user_change_info_2,
                user_change_info_dob.current_dob_match_1,
                user_change_info_dob.current_dob_match_2,
                latest_card_balance_usd)
                
SELECT user_id,
       user_first_name,
       user_last_name,
       account_open_date,
       user_primary_currency,
       user_tag,
       user_kyc_tier,
       is_valid_user,
       is_blocked_by_flag,
       
       user_dob,
       user_age,
       user_birth_country,
       
       user_last_txn_date,
       
       
       user_street,
       blank_info_address,
       
       
       pii_data_source,
       user_change_dob_mismatch,
       dob_error,
       blank_info_birthplace,
       
       gibberish_flag,
       
       latest_fiat_balance_usd,
       latest_crypto_wallet_balance_usd,
       latest_stock_balance_usd,
       latest_card_balance_usd,
       total_user_balance_usd,
       
       months_since_last_txn,
       activity_status,
       
       balance_status_usd,
       
       uos_fake_name_generator,
       
       sequenced_tags,
       
       has_flutterwave_verification,
       
       total_lpv_usd
       
       FROM final