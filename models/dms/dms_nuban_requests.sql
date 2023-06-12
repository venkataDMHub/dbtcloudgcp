SELECT
    nuban_requests.requested_at AS created_at,
    nuban_requests.status AS status,
    nuban_requests.kyc_info_source as kyc_info_source
FROM
    "CHIPPER".{{var("core_public")}}."NUBAN_REQUESTS" AS nuban_requests
