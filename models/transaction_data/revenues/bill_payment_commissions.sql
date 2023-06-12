{{ config(materialized='ephemeral') }}

{% set bill_payment_commission_revenue_rates = {
        'DSTV': 0.9,
        'GOTV': 0.9,
        'StarTimes': 0.9,
        'Ikeja Electric': 0.9,
        'Ibadan Disco Postpaid': 0,
        'Ibadan Disco Prepaid': 0.6,
        'Eko Electricity': 0.4,
        'Port Harcourt Electricity': 0.6,
        'Smile': 1.5,
        'Spectranet': 2
    }
%}

with successful_bill_payments_with_corrected_merchant_name as (
    select
        *,
        iff(biller_name = 'Ibadan Disco', biller_item_name, biller_name) as merchant
    from "CHIPPER".{{ var("core_public") }}."BILL_PAYMENTS"
    where 
        status = 'COMPLETED'
        and reverse_transfer_id is null
)

select
    successful_bill_payments_with_corrected_merchant_name.transfer_id,
    successful_bill_payments_with_corrected_merchant_name.journal_id,
    null as fee_calculation_id,
    null as fee_config_id,
    null as forex_fee_calculation_id,
    null as transfer_quote_id,
    provider as external_provider,
    external_id as external_provider_transaction_id,
		
    hlo_created_at as transaction_created_at,
    hlo_updated_at as transaction_updated_at,

    'BILL_PAYMENT_COMMISSIONS' as revenue_stream,
    currency as revenue_currency,
    null as exchange_rate_fee_percentage_in_decimals,

    case
        {% for merchant, rate in bill_payment_commission_revenue_rates.items() %}
            when merchant = '{{merchant}}' then {{rate}} / 100
        {% endfor %}

        else null
    end as commission_revenue_rate_in_decimals,
    amount * commission_revenue_rate_in_decimals as gross_revenues,
    null as sales_discount_percentage_in_decimals,
    null as sales_discount,
    gross_revenues as net_revenues,

    case when revenue_currency = destination_currency then destination_rate
        when revenue_currency = origin_currency then origin_rate
        else null
    end as rate_to_usd,
    gross_revenues * rate_to_usd as gross_revenues_in_usd,
    sales_discount * rate_to_usd as sales_discount_in_usd,
    net_revenues * rate_to_usd as net_revenues_in_usd,

    user_id as monetized_user_id		
from successful_bill_payments_with_corrected_merchant_name
join "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_TRANSFERS" as expanded_transfers
    on successful_bill_payments_with_corrected_merchant_name.transfer_id = expanded_transfers.transfer_id
