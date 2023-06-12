{{ config(materialized="table") }}

{% set activity_pct = "0.25" %}
{% set user_total_usd = "500" %}
{% set total_interval = "6 days" %}

with
    date_table as (

        select distinct

            cast(created_at as date) - interval '{{total_interval}}' as start_date,
            cast(created_at as date) as end_date

        from chipper.{{ var("core_public") }}.stock_trades

        order by 1
    ),
    stocks as (
        select
            stocks.id,
            stocks.journal_id,
            stocks.transfer_id,
            stocks.user_id,
            stocks.status,
            stocks.position,
            stocks.symbol,
            stocks.currency,
            stocks.amount,
            stocks.shares,
            stocks.amount_in_usd,
            stocks.created_at
        from chipper.{{ var("core_public") }}.stock_trades as stocks
        where stocks.status = 'SETTLED' and stocks.position in ('BUY', 'SELL')
    ),

    activity_by_dt as (

        select
            dates.start_date,
            dates.end_date,
            sum(stocks.amount_in_usd) as total_activity_amt_usd,
            count(stocks.id) as total_activity_cnt,
            count(distinct stocks.user_id) as distinct_user_cnt,
            avg(stocks.amount_in_usd) as avg_activity_amt,
            sum(stocks.amount_in_usd)
            / count(distinct stocks.user_id) as avg_activity_amt_per_user,
            count(stocks.id)
            / count(distinct stocks.user_id) as avg_activity_cnt_per_user

        from date_table as dates

        left join
            stocks
            on cast(stocks.created_at as date)
            between dates.start_date and dates.end_date

        group by 1, 2
        order by 1
    ),

    activity_details as (

        select
            stocks.id,
            stocks.journal_id,
            stocks.user_id,
            stocks.status,
            stocks.position,
            stocks.symbol,
            stocks.currency,
            stocks.amount,
            stocks.shares,
            stocks.amount_in_usd,
            stocks.created_at,

            activity.start_date,
            activity.end_date,
            activity.avg_activity_amt,
            activity.total_activity_amt_usd,
            activity.avg_activity_amt_per_user,
            activity.avg_activity_cnt_per_user,
            activity.distinct_user_cnt,
            activity.total_activity_cnt,
            array_agg(stocks.transfer_id) within group (
                order by stocks.created_at
            ) over (partition by activity.end_date, activity.start_date, stocks.user_id)
            as list_txns,
            sum(stocks.amount_in_usd) over (
                partition by activity.end_date, activity.start_date, stocks.user_id
                order by stocks.created_at
            ) as user_total_usd,
            row_number() over (
                partition by activity.end_date, activity.start_date, stocks.user_id
                order by stocks.created_at
            ) as user_activity_cnt,
            dense_rank() over (
                partition by activity.end_date, activity.start_date, stocks.user_id
                order by stocks.created_at desc
            ) as user_rnk

        from chipper.{{ var("core_public") }}.stock_trades as stocks

        inner join
            activity_by_dt as activity
            on cast(stocks.created_at as date) = activity.end_date

        where stocks.status = 'SETTLED' and stocks.position in ('BUY', 'SELL')

        order by
            activity.start_date, activity.end_date, stocks.user_id, stocks.created_at

    ),

    activity_summary as (

        select
            stocks.start_date,
            stocks.end_date,
            stocks.user_id,
            stocks.currency,
            stocks.user_total_usd,
            stocks.user_activity_cnt,
            (
                stocks.user_total_usd / stocks.user_activity_cnt
            ) as avg_amt_per_activity_for_user,

            stocks.total_activity_cnt,
            stocks.distinct_user_cnt,
            stocks.total_activity_amt_usd,
            stocks.avg_activity_amt,
            stocks.avg_activity_amt_per_user,
            stocks.avg_activity_cnt_per_user,
            stocks.list_txns,

            (stocks.user_total_usd / stocks.user_activity_cnt) / stocks.avg_activity_amt
            - 1 as amt_pct,
            stocks.user_activity_cnt / stocks.avg_activity_cnt_per_user
            - 1 as activity_pct

        from activity_details as stocks
        where
            user_rnk = 1
            and activity_pct >= {{ activity_pct }}
            and user_total_usd >= {{ user_total_usd }}
    )

select user_id, end_date as triggered_at, list_txns as list_of_txns
from activity_summary
