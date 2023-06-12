select
    distinct REGEXP_REPLACE(f.value:"id", '[\'"]', '') as alert_id,
    replace(f.value:"rule", '""', '') as rule_name,
    cast(replace(f.value:"triggered_at", '""', '') as timestamp) as triggered_at,
    user_id 
from
    chipper.{{ var("transaction_monitoring") }}.hummingbird_alerts_audit_trail_with_status,
    lateral flatten(input => parse_json(request_body):"alerts") f
