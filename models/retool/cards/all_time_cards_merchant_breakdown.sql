WITH totals_by_type AS (
    SELECT
        user_id, 
        coalesce(provider_details:MerchantName::text, provider_details:Body:BaseResponse:ResponseData:MerchantName::text) as merchant_name,
        CASE WHEN (type in ('FUNDING', 'WITHDRAWAL') and status = 'COMPLETED') THEN type
            WHEN (type in ('FUNDING', 'WITHDRAWAL') and status <> 'COMPLETED') THEN concat(type, '_', status)
            WHEN type = 'TRANSACTION' and merchant_name <> '' THEN merchant_name
            WHEN type = 'TRANSACTION' and merchant_name = '' THEN concat(type, '_', base_ii_status_definition)
            else 'UNKNOWN'
        END AS merchant,
        sum(abs(amount)) AS total_in_local_currency,
        sum(abs(amount_in_usd)) AS total_usd,
        sum(total_usd) over(partition by user_id) AS total_across_merchants
    FROM {{ref('executed_card_transactions')}}
    GROUP BY user_id, merchant_name, merchant
)
SELECT
    user_id, 
    merchant,
    total_in_local_currency,
    total_usd,
    total_usd / total_across_merchants * 100||'%' as percent_of_total_usd
FROM totals_by_type
