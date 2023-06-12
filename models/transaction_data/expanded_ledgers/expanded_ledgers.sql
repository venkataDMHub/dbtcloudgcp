{{ config(
        materialized='table',
        unique_key='ledger_entry_id',
        on_schema_change='append_new_columns') }}


select * from {{ref('hlo_expanded_ledgers')}}
union
select * from {{ref('dead_end_expanded_ledgers')}}
order by ledger_entry_id

