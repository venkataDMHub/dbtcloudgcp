select
    hlo_id,
    hlo_table,
    hlo_table_with_id,
    transaction_type,
    volume_timestamp,
    is_chipper_processed_volume,
    is_original_transfer_reversed,
    transaction_status,
    origin_currency,
    destination_currency,
    transaction_currency_pair,
    corridor,
    
    max(iff(transaction_side = 'DEBIT', 1, 0)) as has_debit_side,
    max(iff(transaction_side = 'CREDIT', 1, 0)) as has_credit_side,
    has_debit_side + has_credit_side as transaction_sides_count,

    count(distinct iff(transaction_side = 'DEBIT', reference_table_with_id, null)) as count_of_debits,
    count(distinct iff(transaction_side = 'CREDIT', reference_table_with_id, null)) as count_of_credits,
    
    max(iff(transaction_side = 'DEBIT', transfer_id, null)) as debit_side_transfer_id,
    max(iff(transaction_side = 'CREDIT', transfer_id, null)) as credit_side_transfer_id,

    max(iff(transaction_side = 'DEBIT', journal_id, null)) as debit_side_journal_id,
    max(iff(transaction_side = 'CREDIT', journal_id, null)) as credit_side_journal_id,

    max(iff(transaction_side = 'DEBIT', journal_type, null)) as debit_side_journal_type,
    max(iff(transaction_side = 'CREDIT', journal_type, null)) as credit_side_journal_type,

    {# /* Debit-side volume at the user or primary currency level */ #}

        {# /* Debit-side volume type, currency and exchange rates to USD */ #}
    max(iff(transaction_side = 'DEBIT', volume_type, null)) as debit_side_volume_type,
    max(iff(transaction_side = 'DEBIT', volume_currency, null)) as debit_side_currency,
    max(iff(transaction_side = 'DEBIT', volume_rate_to_usd, null)) as debit_side_rate_to_usd,
    max(iff(transaction_side = 'DEBIT', volume_parallel_rate, null)) as debit_side_parallel_rate,
    
        {# /* Debit-side total unadjusted volume */ #}
    sum(iff(transaction_side = 'DEBIT', unadjusted_volume, null)) as debit_side_unadjusted_volume,
    sum(iff(transaction_side = 'DEBIT', unadjusted_volume_in_usd, null)) as debit_side_unadjusted_volume_in_usd,
    sum(iff(transaction_side = 'DEBIT', unadjusted_volume_in_usd_parallel, null)) as debit_side_unadjusted_volume_in_usd_parallel,
    
        {# /* Debit-side total adjusted volume */ #}
    sum(iff(transaction_side = 'DEBIT', adjusted_volume, null)) as debit_side_adjusted_volume,
    sum(iff(transaction_side = 'DEBIT', adjusted_volume_in_usd, null)) as debit_side_adjusted_volume_in_usd,
    sum(iff(transaction_side = 'DEBIT', adjusted_volume_in_usd_parallel, null)) as debit_side_adjusted_volume_in_usd_parallel,
    
        {# /* Debit-side user ID and primary currency */ #}
    max(iff(transaction_side = 'DEBIT', transactor_id, null)) as debit_side_transactor_id,
    max(iff(transaction_side = 'DEBIT', transactor_primary_currency, null)) as debit_side_transactor_primary_currency,

    {# /* Credit-side volume at the user or primary currency level */ #}

        {# /* Credit-side volume type, currency and exchange rates to USD */ #}
    max(iff(transaction_side = 'CREDIT', volume_type, null)) as credit_side_volume_type,
    max(iff(transaction_side = 'CREDIT', volume_currency, null)) as credit_side_currency,
    max(iff(transaction_side = 'CREDIT', volume_rate_to_usd, null)) as credit_side_rate_to_usd,
    max(iff(transaction_side = 'CREDIT', volume_parallel_rate, null)) as credit_side_parallel_rate,
    
        {# /* Credit-side total unadjusted volume */ #}
    sum(iff(transaction_side = 'CREDIT', unadjusted_volume, null)) as credit_side_unadjusted_volume,
    sum(iff(transaction_side = 'CREDIT', unadjusted_volume_in_usd, null)) as credit_side_unadjusted_volume_in_usd,
    sum(iff(transaction_side = 'CREDIT', unadjusted_volume_in_usd_parallel, null)) as credit_side_unadjusted_volume_in_usd_parallel,
    
        {# /* Credit-side total adjusted volume */ #}
    sum(iff(transaction_side = 'CREDIT', adjusted_volume, null)) as credit_side_adjusted_volume,
    sum(iff(transaction_side = 'CREDIT', adjusted_volume_in_usd, null)) as credit_side_adjusted_volume_in_usd,
    sum(iff(transaction_side = 'CREDIT', adjusted_volume_in_usd_parallel, null)) as credit_side_adjusted_volume_in_usd_parallel,
    
        {# /* Credit-side user ID and primary currency */ #}
    max(iff(transaction_side = 'CREDIT', transactor_id, null)) as credit_side_transactor_id,
    max(iff(transaction_side = 'CREDIT', transactor_primary_currency, null)) as credit_side_transactor_primary_currency,
    
    {# /* Volume at the transaction level (Only in USD because of cross-currency transactions) */ #}    

        {# /* Transaction-level total unadjusted volume in USD */ #}
    zeroifnull(debit_side_unadjusted_volume_in_usd) 
        + zeroifnull(credit_side_unadjusted_volume_in_usd) as unadjusted_transaction_volume_in_usd,
        
    zeroifnull(debit_side_unadjusted_volume_in_usd_parallel) 
        + zeroifnull(credit_side_unadjusted_volume_in_usd_parallel) as unadjusted_transaction_volume_in_usd_parallel,
        
        {# /* Transaction-level total adjusted volume in USD */ #}
    zeroifnull(debit_side_adjusted_volume_in_usd) 
        + zeroifnull(credit_side_adjusted_volume_in_usd) as adjusted_transaction_volume_in_usd,
        
    zeroifnull(debit_side_adjusted_volume_in_usd_parallel) 
        + zeroifnull(credit_side_adjusted_volume_in_usd_parallel) as adjusted_transaction_volume_in_usd_parallel
    
from {{ ref('itemized_transaction_volume') }}
{{ dbt_utils.group_by(n=12) }}
