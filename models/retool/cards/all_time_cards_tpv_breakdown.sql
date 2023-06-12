WITH totals_by_type AS (
    SELECT
        user_id,
        CASE WHEN (type in ('FUNDING', 'WITHDRAWAL') and status = 'COMPLETED') THEN type
            WHEN (type in ('FUNDING', 'WITHDRAWAL') and status <> 'COMPLETED') THEN concat(type, '_', status)
            WHEN type = 'TRANSACTION' THEN concat(type, '_', coalesce(base_ii_status_definition, 'UNKNOWN'))
        END AS transaction_type,
        sum(abs(amount)) AS total_in_local_currency,
        sum(abs(amount_in_usd)) AS total_usd,
        sum(total_usd) over(partition by user_id) AS total_across_types
    FROM {{ref('executed_card_transactions')}}
    GROUP BY user_id, type, status, base_ii_status_definition, transaction_type
)
SELECT
    user_id,
    transaction_type,
    total_in_local_currency,
    total_usd,
    total_usd / total_across_types * 100||'%' as percent_of_total_usd
FROM totals_by_type
