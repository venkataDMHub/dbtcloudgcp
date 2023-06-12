{% set P2P_LIST = ('PAYMENTS_P2P_SETTLED','PAYMENT_INVITATIONS_SETTLED', 'REQUESTS_SETTLED') 
%}
{% set REWARDS_LIST =('PAYMENTS_ACTIVATION_TOOLING_PAYOUT_SETTLED', 'PAYMENTS_CASHBACK_SETTLED', 
                      'PAYMENTS_MOBILE_MONEY_CONSUMER_FEE_REIMBURSEMENT_SETTLED','PAYMENTS_REFERRAL_BONUS_SETTLED', 
                      'PAYMENTS_WELCOME_BONUS_SETTLED','PAYMENTS_WELCOME_BONUS_SETTLED', 'STOCK_TRADES_REWARD_SETTLED',
                      'REFERRAL_BONUS_SETTLED', 'CASHBACKS_SETTLED') 
%}
{% set EXCLUDED_TRANSFER_TYPES = ('PAYMENTS_DATA_PURCHASES_REVERSAL_OTHER_SETTLED', 'PAYMENTS_ACCOUNT_MERGES_SETTLED',
                                  'PAYMENTS_BOT_TRANSFERS_SETTLED', 'PAYMENTS_PAYMENTS_FROM_BASE_OTHER_SETTLED', 
                                  'PAYMENTS_COLLECTIONS_TO_BASE_SETTLED','PAYMENTS_ACCOUNT_MERGES_SETTLED', 'CHECKOUTS_SETTLED', 
                                  'PAYMENTS_BOT_TRANSFERS_SETTLED', 'PAYMENTS_WITHDRAWALS_REVERSAL_OTHER_SETTLED')
%}
{% set FINAL_TRANSFER_TYPES = {'ASSET_TRADE_TRANSFERS': ['ASSET_TRADES_BUY', 'ASSET_TRADES_SELL'],
                               'OTHER_TRANSFER_TYPES':  ['AIRTIME_PURCHASES', 'B2C_RECEIVED', 'B2C_SENT', 'BILL_PAYMENTS',
                                                       'C2B_RECEIVED', 'C2B_SENT', 'CHIPPER_REWARDS_PAYOUT', 'CRYPTO_DEPOSITS', 
                                                       'CRYPTO_WITHDRAWALS', 'DATA_PURCHASES', 'DEPOSITS', 
                                                       'ISSUED_CARD_TRANSACTIONS_FUNDING', 'ISSUED_CARD_TRANSACTIONS_WITHDRAWAL',
                                                       'P2P_RECEIVED_CROSS_BORDER', 'P2P_RECEIVED_LOCAL', 'P2P_SENT_CROSS_BORDER', 
                                                       'P2P_SENT_LOCAL', 'STOCK_TRADES_BUY', 'STOCK_TRADES_DIV', 'STOCK_TRADES_DIVTAX', 
                                                       'STOCK_TRADES_SELL', 'WITHDRAWALS']
                                                       }        
%} 											                         
{% set LPV_FINAL_LIST = [] %} 	


with ledgers_cleanup as (

    SELECT DISTINCT l.MAIN_PARTY_USER_ID, l.JOURNAL_ID, l.TRANSFER_ID, 
	 l.LEDGER_AMOUNT_IN_USD,
	CASE 
		WHEN l.TRANSFER_TYPE = 'NETWORK_API_B2C_SETTLED' 
			AND l.LEDGER_AMOUNT_IN_USD <0 
				THEN 'B2C_SENT'
		WHEN l.TRANSFER_TYPE = 'NETWORK_API_B2C_SETTLED' 
			AND l.LEDGER_AMOUNT_IN_USD >0 
				THEN 'B2C_RECEIVED'
		WHEN l.TRANSFER_TYPE = 'NETWORK_API_C2B_SETTLED' 
			AND l.LEDGER_AMOUNT_IN_USD <0 
				THEN 'C2B_SENT'
		WHEN l.TRANSFER_TYPE = 'NETWORK_API_C2B_SETTLED' 
			AND l.LEDGER_AMOUNT_IN_USD >0 
				THEN 'C2B_RECEIVED'
		WHEN l.TRANSFER_TYPE IN {{P2P_LIST}}
			AND l.LEDGER_AMOUNT_IN_USD <0 
            AND l.CORRIDOR = 'CROSS_BORDER_FIAT' 
				THEN 'P2P_SENT_CROSS_BORDER'
		WHEN l.TRANSFER_TYPE IN {{P2P_LIST}}
			AND l.LEDGER_AMOUNT_IN_USD <0 
			AND l.CORRIDOR = 'LOCAL_FIAT' 
				THEN 'P2P_SENT_LOCAL'
		WHEN l.TRANSFER_TYPE IN {{P2P_LIST}}
			AND l.LEDGER_AMOUNT_IN_USD >0 
			AND l.CORRIDOR = 'CROSS_BORDER_FIAT' 
				THEN 'P2P_RECEIVED_CROSS_BORDER'
		WHEN l.TRANSFER_TYPE IN {{P2P_LIST}}
			AND l.LEDGER_AMOUNT_IN_USD >0 
			AND l.CORRIDOR = 'LOCAL_FIAT' 
				THEN 'P2P_RECEIVED_LOCAL'	
		WHEN  l.TRANSFER_TYPE IN {{REWARDS_LIST}}
			THEN 'CHIPPER_REWARDS_PAYOUT' 			
		WHEN l.TRANSFER_TYPE LIKE '%_SETTLED' 
			THEN REGEXP_REPLACE(l.TRANSFER_TYPE, '_SETTLED', '') 
		WHEN l.TRANSFER_TYPE LIKE '%_COMPLETED' 
			THEN REGEXP_REPLACE(l.TRANSFER_TYPE, '_COMPLETED', '')
	END AS TRANSFER_TYPES
	FROM {{ ref('expanded_ledgers') }} l
	WHERE l.IS_ORIGINAL_TRANSFER_REVERSED = FALSE 
	AND l.HLO_STATUS IN ('SETTLED', 'COMPLETED')
	AND l.LEDGER_AMOUNT_IN_USD != 0 -- There are 5 edge cases where a transaction amount = 0 for PAYMENT_INVITATIONS_SETTLED		
	AND l.TRANSFER_TYPE NOT IN {{EXCLUDED_TRANSFER_TYPES}}

), aggregations as ( 

Select
u.USER_ID, 
u.IS_BUSINESS, 
u.IS_VALID_USER,
{% for key, value in FINAL_TRANSFER_TYPES.items() %}
    {% if key == 'ASSET_TRADE_TRANSFERS' %}
        {% for transfer_type in value %}
            SUM(IFF(l.TRANSFER_TYPES = '{{ transfer_type }}', abs(l.LEDGER_AMOUNT_IN_USD), 0))/2  AS TOTAL_{{transfer_type}}_USD,
			COALESCE(count(DISTINCT IFF(l.TRANSFER_TYPES = '{{ transfer_type }}', l.JOURNAL_ID, null)),0)  AS TOTAL_{{transfer_type}}_TRANSFERS,
		{% endfor %}
    {% elif key == 'OTHER_TRANSFER_TYPES' %}  
	  	{% for transfer_type in value %}
            SUM(IFF(l.TRANSFER_TYPES = '{{ transfer_type }}', abs(l.LEDGER_AMOUNT_IN_USD), 0))  AS TOTAL_{{transfer_type}}_USD,
			COALESCE(count(DISTINCT IFF(l.TRANSFER_TYPES = '{{ transfer_type }}', l.JOURNAL_ID, null)),0)  AS TOTAL_{{transfer_type}}_TRANSFERS
		{{ "," if not loop.last}}
		{% endfor %}
    {%  endif %} 
{% endfor %}
From {{ ref('expanded_users') }} u
LEFT JOIN ledgers_cleanup l
	on u.user_id = l.MAIN_PARTY_USER_ID
WHERE u.IS_INTERNAL = FALSE 

{{ dbt_utils.group_by(n=3) }}

) 
Select *,

{% for value in FINAL_TRANSFER_TYPES.values() %}
   {% for transfer_type in value %}
	{% if transfer_type != 'CHIPPER_REWARDS_PAYOUT' %}
     	TOTAL_{{transfer_type}}_USD 
		{{ "+" if not loop.last}}
	{%  endif %} 	 
   {% endfor %}
    {{ "+" if not loop.last}}     
{% endfor %} as SUB_TOTAL_LPV_USD,

 SUB_TOTAL_LPV_USD + TOTAL_CHIPPER_REWARDS_PAYOUT_USD as TOTAL_LPV_USD,
 DIV0(TOTAL_CHIPPER_REWARDS_PAYOUT_USD, TOTAL_LPV_USD)*100 as PERCENT_LPV_FROM_REWARDS,

{% for value in FINAL_TRANSFER_TYPES.values() %}
   {% for transfer_type in value %}
	{% if transfer_type != 'CHIPPER_REWARDS_PAYOUT' %}
     	TOTAL_{{transfer_type}}_TRANSFERS 
		{{ "+" if not loop.last}}
	{%  endif %} 	 
   {% endfor %}
    {{ "+" if not loop.last}}     
{% endfor %} as SUB_TOTAL_LIFETIME_TRANSFERS,

SUB_TOTAL_LIFETIME_TRANSFERS + TOTAL_CHIPPER_REWARDS_PAYOUT_TRANSFERS as TOTAL_LIFETIME_TRANSFERS,
DIV0(SUB_TOTAL_LIFETIME_TRANSFERS, TOTAL_LIFETIME_TRANSFERS)*100 as PERCENT_TRANS_FROM_REWARDS

from aggregations
