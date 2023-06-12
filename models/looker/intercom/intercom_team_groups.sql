{{ config(materialized='ephemeral') }}

WITH teams AS (

SELECT
    teams.*,
    CASE
        WHEN
            teams.name in (
                'Account Access Issues',
                'Account Profile Management',
                'Account Flags/Deletion Request',
                'Network API',
                'Tier II Onboarding Verification',
                'Onboarding/Verification',
                'Tier 2 APM',
                'LRC Flags Escalations'
            )
        THEN 'Account Operations'
        WHEN
            teams.name in (
                'Investments',
                'LRC Stocks'
            )
        THEN 'Wealth Operations'
        WHEN
            teams.name in (
                'Payments Ops Tier 1 Escalations',
                'Payouts',
                'Nuban/Bank Account Transfers',
                'Mobile Money',
                'Charges (Card)',
                'Payments Ops (HOLD)'
            )
        THEN 'Payment Operations'
        WHEN
            teams.name in ('Virtual Cards', 'P2P &amp; S2NC', 'Transfer Ops escalation')
        THEN 'Transfer Operations'
        WHEN
            teams.name
            in ('Referral Escalations', 'Referrals', 'Airtime &amp; Bill Payments')
        THEN 'Marketplace Operations'
        WHEN teams.name in ('Email Support', 'Feedback')
        THEN 'QA + COMMS'
        WHEN teams.name in ('Social media')
        THEN 'Social Media'

        ELSE teams.name

    END AS team_group
FROM chipper.intercom.team AS teams
ORDER BY name
)

SELECT
teams.team_group AS team_group,
teams.name AS team_name,
teams.id AS team_id
FROM teams
ORDER BY team_group, team_name
