SELECT
    bill.created_at AS created_at,
    bill.status AS STATUS,
    bill.provider AS provider,
    bill.biller_name,
    bill.biller_item_name,
    bill.currency AS currency,
    bill.payment_response,
    bill.payment_response :errorMessage AS error_message
FROM
    "CHIPPER".{{var("core_public")}}."BILL_PAYMENTS" AS bill
