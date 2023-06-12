{% set months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] %}

select
    expanded_users.user_id,
    date_trunc('month', expanded_users.created_at) as acquisition_month,
    1 as month_0,

    {% for month in months %}
            max(
                case when date_trunc('month', acquisition_month + interval '{{month}} month') = date_trunc('month', event_date) then 1 
                    else 0
                end
            ) as month_{{month}}
        {{"," if not loop.last}}
    {% endfor %}
from "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_USERS" as expanded_users
left join {{ref('users_opened_app')}} as users_opened_app on expanded_users.user_id = users_opened_app.user_id
group by 1, 2, 3
