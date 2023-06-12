{% set NUM_PERCENTILE_BUCKETS  = 200 %}
{% for percentile_bucket_index in range(NUM_PERCENTILE_BUCKETS ) %}
{% set percentile = percentile_bucket_index / NUM_PERCENTILE_BUCKETS %}
  select
	{{ percentile }} as percentile,
	APPROX_PERCENTILE( ONBOARDING_TIME , {{ percentile }} ) AS ONBOARDING_TIME,
	APPROX_PERCENTILE( ONBOARDING_NUM_IP , {{ percentile }} ) AS NUM_IP,
	APPROX_PERCENTILE( ONBOARDING_USER_PER_IP_COUNT , {{ percentile }} ) AS USER_PER_IP_COUNT
  from {{ ref('uos_recent_data')}}
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
