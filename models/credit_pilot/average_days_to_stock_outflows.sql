{{ config(materialized = 'ephemeral') }}

 with stock_inflow as (

    select
        user_id,
        transfer_id,
        created_at,
        symbol,
        position,
        shares
    from chipper.{{ var("core_public") }}.stock_trades
    where status = 'SETTLED'
        and position = 'BUY'

),

stock_outflow as (

    select
        user_id,
        transfer_id,
        created_at,
        symbol,
        position,
        shares
    from chipper.{{ var("core_public") }}.stock_trades
    where status = 'SETTLED'
        and position = 'SELL'

),

days_to_next_stock_outflow as (

    select
        i.user_id,
        i.transfer_id,
        i.created_at,
        i.symbol,
        i.position,
        i.shares,
        o.created_at as next_outflow_transaction_date,
        datediff('days', i.created_at, o.created_at ) as days_to_next_outflow
    from stock_inflow as i
    left join stock_outflow as o
        on i.user_id = o.user_id
            and i.symbol = o.symbol
            and i.created_at < o.created_at
    qualify days_to_next_outflow = min(days_to_next_outflow) over(partition by i.transfer_id)



),

stock_final as (

    select
        user_id,
        avg(days_to_next_outflow) as average_days_between_stock_sells_and_buys
    from days_to_next_stock_outflow
    group by 1

)

select *
from stock_final
