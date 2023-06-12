select 
    case_token as case_id,
    cast(replace(parse_json(feedback):"case":"created_at", '""', '') as timestamp) as case_created_at,
    cast(replace(parse_json(feedback):"case":"updated_at", '""', '') as timestamp) as case_updated_at,
    f.value:"alerts" as alert_info,
    f.value:"decisions" as decisions,
    replace(f.value:"status", '""', '') as status,
    f.value:"assignee" as analyst_details,
    cast(replace(f.value:"completed_at", '""', '') as timestamp) as completed_at,
    user_id,
    f.value:"alerts" as alerts_triggered,
    replace(f.value:"token", '""', '') as review_token,
    cast(replace(f.value:created_at, '""', '') as timestamp) as review_created_at,
    cast(replace(f.value:"first_activity_at", '""', '') as timestamp) as review_first_activity,
    replace(f.value:"workflow",'"','') as review_stage
from chipper.{{var("transaction_monitoring")}}.hummingbird_case_feedback,
lateral flatten(input=> parse_json(feedback):"case":"reviews") f
qualify row_number() over (partition by review_token order by review_token) = 1
