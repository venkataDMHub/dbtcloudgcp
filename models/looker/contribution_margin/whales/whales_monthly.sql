{{ config(materialized='table', schema='looker') }}

with
    user_details as (

        select
            date_trunc('month', transaction_updated_at) as settled_month,
            monetized_user_id,
            revenue_stream,
            net_revenues_in_usd,
            net_revenues_in_usd_parallel

        from chipper.dbt_transformations_looker.revenue_details

    ),

    monthly_summary as (

        select

            settled_month,
            revenue_stream,
            sum(net_revenues_in_usd) as monthly_net_revenues_in_usd,
            sum(net_revenues_in_usd_parallel) as monthly_net_revenues_in_usd_parallel

        from user_details
        group by 1, 2
        order by 1, 2

    ),

    user_monthly_revenue_share as (

        select

            user_details.settled_month,
            user_details.revenue_stream,
            user_details.monetized_user_id,

            monthly_summary.monthly_net_revenues_in_usd,
            monthly_summary.monthly_net_revenues_in_usd_parallel,

            sum(user_details.net_revenues_in_usd) as user_monthly_net_revenues_in_usd,
            sum(user_details.net_revenues_in_usd_parallel) as user_monthly_net_revenues_in_usd_parallel,

            sum(user_monthly_net_revenues_in_usd) over (
                partition by user_details.settled_month, user_details.revenue_stream
                order by user_monthly_net_revenues_in_usd desc
            ) as cumulative_revenues_in_usd,
            sum(user_monthly_net_revenues_in_usd_parallel) over (
                partition by user_details.settled_month, user_details.revenue_stream
                order by user_monthly_net_revenues_in_usd_parallel desc
            ) as cumulative_revenues_in_usd_parallel,

            cumulative_revenues_in_usd / monthly_net_revenues_in_usd as percent_revenue_share,
            cumulative_revenues_in_usd_parallel / monthly_net_revenues_in_usd_parallel as percent_revenue_share_parallel,

            iff(percent_revenue_share <= .8, TRUE, FALSE) as is_whale,
            iff(percent_revenue_share_parallel <= .8, TRUE, FALSE) as is_parallel_whale

        from user_details

        left join
            monthly_summary

            on user_details.settled_month = monthly_summary.settled_month
            and user_details.revenue_stream = monthly_summary.revenue_stream

        {{ dbt_utils.group_by(n=5) }}
    )

select *
from user_monthly_revenue_share
