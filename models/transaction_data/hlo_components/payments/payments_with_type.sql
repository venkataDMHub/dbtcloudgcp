{{ config(materialized='ephemeral') }}

select *
from {{ ref('user_to_user_payments') }}

union

select *
from {{ ref('payments_with_bot') }}

union

select *
from {{ ref('base_to_base_payments') }}

union

select *
from {{ ref('collections_to_base') }}

union

select *
from {{ ref('payments_from_base') }}

order by
    hlo_id,
    transfer_id,
    hlo_journal_id,
    hlo_created_at,
    hlo_updated_at
