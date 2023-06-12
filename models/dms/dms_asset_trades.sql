SELECT
    asset.created_at as created_at,
    asset.status as status,
    asset.fee_currency,
    asset.position as position,
    asset.fee_amount,
    asset.status_message as error_message,
    asset.asset as asset
FROM {{ref('asset_trades')}}  as asset
