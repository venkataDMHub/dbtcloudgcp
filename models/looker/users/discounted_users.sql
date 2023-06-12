{{ config(materialized='table', schema='looker') }}

with risk_flags as (

    select distinct
        user_id,
        risk_type as discount_category,
        risk_reason as flag_reason
    from {{ ref('risk_flags') }}

),

deleted_users as (

    select distinct
        user_id,
        'DELETED_USER' as discount_category,
        'DELETED_USER' as flag_reason
    from {{ ref('expanded_users') }}
    where is_deleted = TRUE

),

invalid_users as (

    select
        user_id,
        'INVALID_USER' as discount_category,
        reason as flag_reason
    from {{ ref('all_invalid_user_ids') }}

),

unioned_table as (

    select
        user_id,
        discount_category,
        flag_reason
    from risk_flags

    union

    select
        user_id,
        discount_category,
        flag_reason
    from deleted_users

    union

    select
        user_id,
        discount_category,
        flag_reason
    from invalid_users

),

final as (

    select f.*
    from unioned_table as f
    inner join {{ ref('expanded_users') }} as u
        on f.user_id = u.user_id

)

select *
from final
