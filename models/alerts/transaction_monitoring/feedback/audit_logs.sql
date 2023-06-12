select
    replace(f.value:"token",'"','') as review_token,
    cast(replace(f.value:"created_at",'"','') as timestamp) as created_at,
    replace(f.value:"assignee":"name",'"','') as analyst_name,
    f.value:"decisions" as decisions,
    replace(f.value:"decisions"[0]:"action_name",'"','') as action_name,
    replace(f.value:"decisions"[0]:"choice_id",'"','') as choice_id,
    replace(f.value:"decisions"[0]:"choice_name",'"','') as choice_name,
    f.value:"text_entries" as comments_notes,
    cast(replace(f.value:"decisions"[0]:"decision_made_at",'"','') as timestamp) as decision_made_at,
    cast(replace(f.value:"completed_at",'"','') as timestamp) as completed_at,
    replace(f.value:"internal_control_number",'"','') as internal_control_number,
    replace(f.value:"workflow", '"','') as stage
from 
chipper.{{var('transaction_monitoring')}}.hummingbird_case_feedback,
lateral flatten (input => parse_json(feedback):"case":"reviews")f
qualify row_number() over (partition by review_token order by review_token) = 1
