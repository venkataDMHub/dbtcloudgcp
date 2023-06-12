select
    user_id,
    max(event_type) as unique_event_type,
    date_trunc('day', event_time) as event_date
from "CHIPPER"."AMPLITUDE"."EVENTS_204512"
group by
    user_id,
    event_type,
    event_date
order by
    user_id,
    event_date,
    event_type
