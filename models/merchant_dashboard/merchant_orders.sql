{{  config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns') }}

with order_with_authorisation as (
  select
    o.id,
    max(uaa.id) authorisation_id
  from
    {{ var('core_public') }}.orders o
  left join
    {{ var('core_public') }}.user_account_authorisations uaa
  on o.id = uaa.order_id
  group by o.id
)

select
  o.id,
  o.status,
  o.created_at,
  o.updated_at,
  o.merchant_reference,
  concat(u2.first_name, ' ', u2.last_name) merchant_name,
  u1.tag customer_tag,
  u1.primary_currency customer_primary_currency,
  t.origin_amount,
  t.origin_currency,
  t.destination_amount,
  t.destination_currency,
  owa.authorisation_id,
  o.merchant_id,
  o.note
from
  {{ var('core_public') }}.orders o
left join
  {{ var('core_public') }}.transfers t
on o.transfer_id = t.id
left join
  {{ var('core_public') }}.users u1
on u1.id = o.payer_id
left join
  {{ var('core_public') }}.users u2
on u2.id = o.merchant_id
left join
  order_with_authorisation owa
on o.id = owa.id

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where o.updated_at >= (select dateadd(day, - 1, max(updated_at)) from {{ this }})
{% endif %}
