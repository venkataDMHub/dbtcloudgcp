{{  config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns') }}

select
  p.id,
  p.status,
  p.created_at,
  p.updated_at,
  concat(u2.first_name, ' ', u2.last_name) merchant_name,
  p.reference merchant_reference,
  u1.tag customer_tag,
  u1.primary_currency customer_primary_currency,
  t.origin_amount,
  t.origin_currency,
  t.destination_amount,
  t.destination_currency,
  p.sender_id merchant_id,
  p.note
from
  {{ var('core_public') }}.payments p
left join
  {{ var('core_public') }}.transfers t
on p.transfer_id = t.id
left join
  {{ var('core_public') }}.users u1
on u1.id = p.recipient_id
left join
  {{ var('core_public') }}.users u2
on u2.id = p.sender_id
where p.payment_grouping = 'NETWORK_API_PAYOUT'

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  and p.updated_at >= (select dateadd(day, - 1, max(updated_at)) from {{ this }})
{% endif %}
