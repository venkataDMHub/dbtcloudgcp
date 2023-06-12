{{
    config(
        materialized='incremental',
        unique_key='user_id',
        schema='intermediate'
    )
}}

with check_facescan as (
    select distinct user_id
    from {{ var("compliance_public") }}.facetec_facescans
    where
        check_success = True
        {% if is_incremental() %}
            and user_id not in (select user_id from {{ this }})
        {% endif %}
),

check_liveness as (
    select distinct user_id
    from {{ var("compliance_public") }}.liveness_checks
    where
        status = 'ACCEPTED'
        {% if is_incremental() %}
            and user_id not in (select user_id from {{ this }})
        {% endif %}
)

(
    select
        user_id,
        True as has_selfie_checked
    from check_facescan
)
union distinct
(
    select
        user_id,
        True as has_selfie_checked
    from check_liveness
)
