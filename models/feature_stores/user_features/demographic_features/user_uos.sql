{{
    config(
        materialized='ephemeral'
    )
}}

-- This is because UOS has a known issue that there will be duplicate rows
with ranked_uos as (
    select
        user_id,
        uos_score,
        uos_score_v3,
        onboarding_time,
        row_number() over (
            partition by user_id order by uos_created_at desc
        ) as row_num
    from chipper.utils.user_onboarding_score
)

select
    user_id,
    uos_score,
    uos_score_v3,
    onboarding_time
from ranked_uos
where row_num = 1
