{{ config(materialized='ephemeral') }}

select * from {{ ref('deposits_with_bank_charges') }}
union
select * from {{ ref('deposits_with_card_charges') }}
union
select * from {{ ref('deposits_with_charges') }}
union
select * from {{ ref('deposits_with_deposit_webhooks_receipts') }}
