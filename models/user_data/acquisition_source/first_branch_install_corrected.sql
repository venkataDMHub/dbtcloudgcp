{%
set advertising_partner_mapping = { 'undisclosed': 'Facebook',
    'Snapchat': 'Snap' } %}

select
    user_id,
    case
        {% for key, value in advertising_partner_mapping.items() %}
        when acquisition_source like '{{key}}' then '{{value}}'
        {% endfor %}
        else acquisition_source end as acquisition_source,
    campaign,
    ad_set_id,
    ad_set_name,
    branch_install_created_at,
    branch_install_id

from {{ ref('first_branch_install') }}
