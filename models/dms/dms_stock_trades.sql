SELECT
    stock.created_at AS created_at,
    stock.status AS STATUS,
    stock.position AS position,
    stock.symbol as symbol,
    stock.currency as currency,
    stock.status_message AS status_message
FROM
    "CHIPPER".{{var("core_public")}}."STOCK_TRADES" AS stock
