select
    user_id,
    listagg(distinct reason, ', ') within group (order by reason) as invalid_user_reasons
from {{ ref('all_invalid_user_ids') }}
group by user_id
