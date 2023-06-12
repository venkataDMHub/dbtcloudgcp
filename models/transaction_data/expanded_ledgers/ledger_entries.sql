{{ config(materialized='ephemeral') }}

select * 
from chipper.{{ var("core_public") }}.ledger_entries
