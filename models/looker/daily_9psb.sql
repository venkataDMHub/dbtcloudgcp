{{ config(materialized='table', schema='looker') }}

WITH    
    "9psb_nubans" AS (
        SELECT 
            linked_account_id,
            nuban_details.value:nubanBankName::text AS nuban_bank_name,
            nuban_details.value:nubanAccountNumber::text AS nuban_account_number,
            nuban_details.value:nubanAccountName::text AS nuban_account_name,
            nuban_details.value:nubanIsActive::boolean AS nuban_is_active,
            linked_accounts.user_id,
            linked_accounts.created_at AS nuban_created_at
        FROM {{ref('payment_methods')}} 
        LEFT JOIN {{var('core_public')}}.LINKED_ACCOUNTS ON payment_methods.linked_account_id = linked_accounts.id,
        lateral flatten(input => payment_methods.payment_method_details:nubanPaymentMethodDetails) AS nuban_details
        WHERE 
            nuban_bank_name = '9 Payment Service Bank'
            AND nuban_is_active = true
        ORDER BY 1, 2, 3, 4, 5, 6
    ),

    ledger_running_balance AS (
        SELECT
            main_party_user_id,
            ledger_entry_id,
            ledger_timestamp,
            ledger_currency,
            ledger_amount,
           SUM(ledger_amount) OVER (
                partition BY
                    main_party_user_id,
                    ledger_currency
                ORDER BY
                    ledger_entry_id,
                    ledger_timestamp
            ) AS running_balance,

            ROW_NUMBER() OVER (
                partition BY
                    main_party_user_id,
                    ledger_currency
                ORDER BY
                    ledger_entry_id DESC,
                    ledger_timestamp DESC
            ) AS row_num_desc
        FROM {{ref('expanded_ledgers')}} 
        WHERE 
            ledger_currency = 'NGN' 
            AND main_party_user_id IN (SELECT user_id FROM "9psb_nubans")
            AND ledger_timestamp <= CURRENT_TIMESTAMP()
        ORDER BY
            main_party_user_id,
            ledger_entry_id,
            ledger_timestamp
    ),

    min_row_num_desc AS (
        SELECT 
            main_party_user_id AS main_party_user_id_2,
            MIN(row_num_desc) AS min_row_num_desc
        FROM ledger_running_balance
        WHERE ledger_timestamp <= DATEADD('day', -1, CURRENT_TIMESTAMP())
        GROUP BY main_party_user_id_2
    ),

    opening_balance AS (
        SELECT 
            *,
            running_balance AS opening_balance
        FROM ledger_running_balance
        JOIN min_row_num_desc 
            ON (
                ledger_running_balance.row_num_desc = min_row_num_desc.min_row_num_desc
                AND ledger_running_balance.main_party_user_id = min_row_num_desc.main_party_user_id_2
            )
    ),

    closing_balance AS (
        SELECT 
            *,
            running_balance AS closing_balance
        FROM ledger_running_balance
        WHERE row_num_desc = 1
    ),

    activities AS (
        SELECT
            ledger_running_balance.main_party_user_id,
            SUM(CASE WHEN ledger_running_balance.ledger_amount < 0 THEN ABS(ledger_running_balance.ledger_amount) ELSE 0 END) AS sum_debits,
            SUM(CASE WHEN ledger_running_balance.ledger_amount > 0 THEN ABS(ledger_running_balance.ledger_amount) ELSE 0 END) AS sum_credits
        FROM ledger_running_balance
        WHERE
            ledger_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        GROUP BY ledger_running_balance.main_party_user_id
    ),

    main_query AS (
        SELECT
            nuban_account_number,
            COALESCE(opening_balance, 0) AS opening_balance,
        --   COALESCE(sum_debits, 0) AS total_debits,
        --   COALESCE(sum_credits, 0) AS total_credits,
            COALESCE(closing_balance, 0) AS closing_balance,
            closing_balance - opening_balance AS ledger_change,
            CONCAT('9psb-', all_users.user_id) AS client_code_customer_id,
            ROW_NUMBER() OVER (ORDER BY all_users.user_id) AS row_num
        FROM "9psb_nubans" AS all_users
        LEFT JOIN opening_balance ON all_users.user_id = opening_balance.main_party_user_id
        --LEFT JOIN activities ON all_users.user_id = activities.main_party_user_id
        LEFT JOIN closing_balance ON all_users.user_id = closing_balance.main_party_user_id
        ORDER BY
            closing_balance desc,
            opening_balance desc,
        --    sum_credits desc,
        --    sum_debits desc,
            client_code_customer_id
    ) 
SELECT * 
FROM main_query
