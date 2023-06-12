
SELECT
    airtime.created_at as created_at,
    airtime.status as status,
    airtime.airtime_provider as airtime_provider,
    airtime.phone_country_code as phone_country_code,
    airtime.phone_carrier as phone_carrier,
    airtime.provider_response :errorMessage as error_message
FROM
    {{ref('airtime_purchases')}}  as airtime
