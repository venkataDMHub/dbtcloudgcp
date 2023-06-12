select
    created_at,
    status,
    country,
    case when sms_provider is null then 'EMAIL' else sms_provider end as provider
from
    CHIPPER.{{var("auth_public")}}.SENT_OTP
