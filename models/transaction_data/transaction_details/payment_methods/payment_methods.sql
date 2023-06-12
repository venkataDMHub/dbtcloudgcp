select * from {{ ref('bank_account_linked_accounts') }}
union
select * from {{ ref('mobile_money_linked_accounts') }}
union
select * from {{ ref('nuban_linked_accounts') }}
union
select * from {{ ref('payment_cards_linked_accounts') }}
union
select * from {{ ref('railsbank_linked_accounts') }}



