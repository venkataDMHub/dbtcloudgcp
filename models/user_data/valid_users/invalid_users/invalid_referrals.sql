{{ config(materialized = 'ephemeral') }}

{% set referral_flags =  {'NEEDS_REVIEW' : 'NEEDS_REVIEW_INVITED_USER', 
                       'FLAGGED_BY_UOS': 'FLAGGED_BY_UOS_INVITED_USER', 
                       'FLAGGED_BY_SARDINE': 'FLAGGED_BY_SARDINE_INVITED_USER', 
                        'FLAGGED_BY_FAKE_NAME_RATIO': 'FLAGGED_BY_FAKE_NAME_RATIO_INVITED_USER', 
                        'FLAGGED_BY_OTHER': 'FLAGGED_BY_OTHER_INVITED_USER',
                        'FLAGGED_BY_SIMILAR_SELFIE': 'FLAGGED_BY_SIMILAR_SELFIE_INVITED_USER'} %}



SELECT
    referrals.invited_user_id AS user_id,
    CASE
        {% for key, value in referral_flags.items() %} 
        WHEN referrals.status = '{{key}}' THEN '{{value}}'
                {% endfor %}
    END AS reason

FROM chipper.{{ var("core_public") }}.referrals
WHERE referrals.status IN (
    {% for key, value in referral_flags.items() %} 
    '{{key}}'
    {{ "," if not loop.last}}
        {% endfor %}
)
