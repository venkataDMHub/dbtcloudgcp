SELECT
    stwh.transfer_id,
    stwh.journal_id,
    stwh.created_at,
    stwh.journal_type,
    stwh.transfer_type,
    stwh.origin_currency,
    stwh.origin_amount,
    stwh.destination_currency,
    stwh.destination_amount,
    stwh.origin_amount_in_usd,
    stwh.destination_amount_in_usd,
    stwh.outgoing_user_id,
    stwh.outgoing_tag,
    stwh.outgoing_user_device_id,
    stwh.incoming_user_id,
    stwh.incoming_tag,
    stwh.transaction_details,
    PERCENT_RANK() OVER(
        PARTITION BY outgoing_user_id ORDER BY origin_amount_in_usd ASC
    ) AS percentile_rank,
    COUNT(transfer_id) OVER(PARTITION BY incoming_user_id, outgoing_user_id) AS between_user_transact_count,
    CASE
        WHEN outgoing_user_device_id LIKE 'base%' OR incoming_user_id LIKE 'base%' THEN TRUE
        ELSE FALSE
    END AS is_interacted_base
FROM
    chipper.transformed.settled_transfers_with_hlo_2 AS stwh
WHERE
    (hlo_status = 'SETTLED' OR hlo_status = 'COMPLETED')
