{{ config(materialized='ephemeral') }}

{# /* Many transactions, especially older ones and manually-credited ones, don't have a payment grouping.
    But, we can handle the types by: 
        - Either finding the note string used for payFromBase use cases in ChipperCore
        - Or inferring from the note string patterns. 

    1. WELCOME_BONUS from ChipperCore/app/controllers/reward/index.ts (issueOnboardingCompletionAirdrop function).
    2. REFERRAL_BONUS from either ChipperCore/app/controllers/referral/processReferral.ts (getBonusAmounts and processReferral functions)
        or from querying the payments table for notes used in very old referrals.
    3. MOBILE_MONEY_CONSUMER_FEE_REIMBURSEMENT from ChipperCore/app/controllers/deposit/index.ts (refundConsumerFeesIfNecessary function).
    4. DATA_PURCHASES_REVERSAL_OTHER from ChipperCore/app/controllers/data_bundles/index.ts (processDataPurchaseRefund function).
    5. WITHDRAWALS_REVERSAL_OTHER from ChipperCore/app/controllers/withdrawals/index.ts (refundWithdrawal function)
        or from querying the payments table for notes used in manually-credited refunds.
*/ #}

{% set inferred_type_by_note = {
    '%welcome to chipper cash%': 'WELCOME_BONUS',
    '%to get you started%': 'WELCOME_BONUS',
    '%airdrop%': 'WELCOME_BONUS',

    '%from your referral link%': 'REFERRAL_BONUS',
    '%successfully referring%': 'REFERRAL_BONUS',
    '%reward for referring%': 'REFERRAL_BONUS',
    '%created an account using your link%': 'REFERRAL_BONUS',
    '%for inviting your friend%': 'REFERRAL_BONUS',
    '%great job with the invitations%': 'REFERRAL_BONUS',
    '%for the invite you sent to%': 'REFERRAL_BONUS',
    '%through your invitation%': 'REFERRAL_BONUS',
    '%inviting%': 'REFERRAL_BONUS',
    '%congratulations on joining chipper cash and verifying your account. welcome aboard%': 'REFERRAL_BONUS',
    '%via his referral link%': 'REFERRAL_BONUS',
    '%referred a friend successfully%': 'REFERRAL_BONUS',
    '%complete%referral%': 'REFERRAL_BONUS',
    '%missing%referral%': 'REFERRAL_BONUS',
    '%referring%your%': 'REFERRAL_BONUS',

    'this is to cover the%fee%deposit%': 'MOBILE_MONEY_CONSUMER_FEE_REIMBURSEMENT',
    'this is to cover%mpesa%': 'MOBILE_MONEY_CONSUMER_FEE_REIMBURSEMENT',
    'this is to cover%mtn%': 'MOBILE_MONEY_CONSUMER_FEE_REIMBURSEMENT',

    '%your data bundle purchase%failed%refunded%': 'DATA_PURCHASES_REVERSAL_OTHER',

    '%unable to process%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%reversed the transaction%make sure%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%unable to process%withdraw%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%not process%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%unable%complete%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%cash%out%delay%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%cashout%delay%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%withdraw%delay': 'WITHDRAWALS_REVERSAL_OTHER',
    '%delay%withdraw%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%delay%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%fail%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%fail%cashout%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%fail%withdraw%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%cash%out%fail%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%withdraw%fail%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%cash%out%reimburse%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%cash%out%refund%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%withdraw%refund%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%refund%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%refund%withdraw%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%revers%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%revers%withdraw%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%withdrawal%not%deliver%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%cash%out%cannot%be%deliver%': 'WITHDRAWALS_REVERSAL_OTHER',
    'unfortunately%cash%out%': 'WITHDRAWALS_REVERSAL_OTHER',
    'unfortunately%withdraw%': 'WITHDRAWALS_REVERSAL_OTHER',
    '%cash%out%inconvenience%': 'WITHDRAWALS_REVERSAL_OTHER',
    }
%}

with all_transfer_ids as (
    select
        payments.id as hlo_id,
        'PAYMENTS' as hlo_table,
        payments.sender_id,
        payments.recipient_id,
        payments.status as hlo_status,
        payments.transfer_id as original_transfer_id,
        payments.journal_id as original_transfer_journal_id,
        payments.created_at as hlo_created_at,
        payments.updated_at as hlo_updated_at,
        payments.error_message,
        payments.short_id,
        payments.reference,
        payments.note,
        payments.payment_grouping,
        case
            when referrals.transfer_id is not null then concat('REFERRAL_BONUS_', payments.status)
            when payments.payment_grouping = 'REFERRAL_BONUS' then concat('REFERRAL_BONUS_', payments.status)
            
            {# /* Handle deposits classified as payments. The note string always has the phrase "Incoming Payment or Incoming Funds".
                More on this in the following places in ChipperCore:
                - https://github.com/ChipperCash/ChipperCore/pull/1497
                - ChipperCore/app/controllers/rave/webhook.ts (creditNubanAccount function)
                - ChipperCore/app/controllers/paystack/webhook.ts (handlePaystackBankChargeCompleted function)
                - ChipperCore/app/controllers/railsbank/index.ts (handleTransactionTypeReceive function)
                - ChipperCore/app/controllers/monnify/webhook.ts (catchAllMonnifyWebhooks function)
                - ChipperCore/app/controllers/feed/format.ts (formatNubanDepositsAsSettledCards function) */ #}
            when payments.payment_grouping = 'BANK_PAYMENT' then concat('DEPOSITS_', payments.status)
            when lower(payments.note) like any ('%incoming payment%', '%incoming funds%') then concat('DEPOSITS_', payments.status)
            when payments.payment_grouping = 'CASHBACK' then concat('CASHBACKS_', payments.status)
            when payments.payment_grouping = 'ACTIVATION_TOOLING_PAYOUT' then concat(hlo_table, '_ACTIVATION_TOOLING_PAYOUT_', payments.status)
            
            {% for key, value in inferred_type_by_note.items() %}
            when lower(payments.note) like any ('{{key}}') then concat(hlo_table, '_{{value}}_', payments.status)
            {% endfor %}
            
            else concat(hlo_table, '_PAYMENTS_FROM_BASE_OTHER_', payments.status)
        end as original_transfer_payment_type,

        payments.reverse_transfer_id,
        reverse_transfers.journal_id as reverse_journal_id,
        case when payments.reverse_transfer_id is null then false
            else true
        end as is_original_transfer_reversed,
        'PAYMENTS_FROM_BASE_REVERSAL' as reverse_transfer_payment_type
    from
        "CHIPPER".{{ var("core_public") }}."PAYMENTS" as payments
    left join
        {{ ref('referrals_with_all_transfer_ids') }} as referrals on payments.transfer_id = referrals.transfer_id
    left join
        "CHIPPER".{{ var("core_public") }}."TRANSFERS" as reverse_transfers 
            on payments.reverse_transfer_id = reverse_transfers.id
    where
        payments.sender_id like 'base-%'
        and payments.recipient_id not like 'base-%'
        and payments.recipient_id not like 'bot-%'
        and payments.transfer_id not in (select * from {{ ref('reversals_handled_in_other_modules') }})
        and payments.transfer_id is not null
),

transfer_ids as (
    select
        hlo_id,
        hlo_table,
        original_transfer_journal_id as hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        original_transfer_id as transfer_id,
        is_original_transfer_reversed,
        false as is_transfer_reversal,
        original_transfer_payment_type as payment_type
    from all_transfer_ids
),

reverse_transfer_ids as (
    select
        hlo_id,
        hlo_table,
        reverse_journal_id as hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        true as is_transfer_reversal,
        reverse_transfer_payment_type as payment_type
    from all_transfer_ids
    where is_original_transfer_reversed = true
)

select *
from transfer_ids

union

select *
from reverse_transfer_ids
