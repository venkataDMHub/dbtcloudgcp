with
    audit as (
        select user_id, alert_send_time, request_body, response
        from
            chipper.{{ var("transaction_monitoring") }}.hummingbird_alerts_audit_trail_with_status
        union
        select *
        from chipper.{{ var("transaction_monitoring") }}.hummingbird_alerts_audit_trail
    ),
    flatten_sql as (
        select distinct
            user_id,
            replace(f.value:"rule", '""', '') as rule_name,
            replace(f.value:"triggered_at", '""', '') as triggered_at
        from audit, lateral flatten(input => parse_json(request_body):"alerts") f
    )
select
    user_id,
    rule_name,
    triggered_at,
    cast(
        regexp_substr(triggered_at, '^[0-9]{4}-[0-9]{2}-[0-9]{2}') as date
    ) as triggered_date
from flatten_sql
qualify
    row_number() over (partition by user_id, rule_name order by triggered_at desc) = 1
