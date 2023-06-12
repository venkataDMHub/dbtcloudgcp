{{ config(materialized='table', schema='looker') }}

WITH admin_teams AS (    
  SELECT 
    admins.id,
    admins.name, 
    LISTAGG(teams.name, ', ') AS teams,
    LISTAGG(team_groups.team_group, ', ') AS team_groups
  FROM "CHIPPER"."INTERCOM"."ADMIN" as admins
      LEFT JOIN "CHIPPER"."INTERCOM"."TEAM_ADMIN" as team_admins 
      ON team_admins.admin_id = admins.id
      AND team_admins._fivetran_deleted = 'FALSE'
      LEFT JOIN "CHIPPER"."INTERCOM"."TEAM" as teams 
      ON teams.id = team_admins.team_id
      LEFT JOIN {{ref('intercom_team_groups')}} as team_groups 
      ON teams.id = team_groups.team_id
  GROUP BY admins.id, admins.name
),
latest_tickets AS (
    SELECT 
        id,
        updated_at,
        created_at,
        open,
        state,
        read,
        waiting_since,
        snoozed_until,
        priority,
        source_type,
        source_id,
        source_delivered_as,
        source_subject,
        source_body,
        source_url,
        source_author_type,
        source_author_id,
        assignee_id,
        assignee_type,
        first_contact_reply_type,
        first_contact_reply_url,
        first_contact_reply_created_at,
        conversation_rating_remark,
        conversation_rating_created_at,
        conversation_rating_value,
        ROW_NUMBER() OVER (
          PARTITION BY id
          ORDER BY updated_at DESC
        ) AS rnk
    FROM CHIPPER.INTERCOM.CONVERSATION_HISTORY
    QUALIFY rnk = 1
),

latest_conversation_contact_history AS (
    SELECT 
    contact_id,
    conversation_id,
    ROW_NUMBER() OVER (
          PARTITION BY conversation_id
          ORDER BY conversation_updated_at DESC
        ) AS rnk
    FROM "CHIPPER"."INTERCOM"."CONVERSATION_CONTACT_HISTORY"
    QUALIFY rnk = 1
),

latest_contact_history AS (
    SELECT 
      id,
      external_id,
      location_country,
      location_region,
      custom_primary_currency,
      ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY updated_at DESC
          ) AS rnk
    FROM "CHIPPER"."INTERCOM"."CONTACT_HISTORY"
    QUALIFY rnk = 1
),

conversation_tag_id AS (
  SELECT 
    conversation_id,
    tag_id,
    ROW_NUMBER() OVER (
        PARTITION BY conversation_id
        ORDER BY conversation_updated_at DESC
    ) AS rnk
  FROM "CHIPPER"."INTERCOM"."CONVERSATION_TAG_HISTORY"
  QUALIFY rnk = 1
),

number_of_comments AS (
  SELECT 
    conversation_id,
    SUM(CASE WHEN author_type = 'bot' THEN 1 ELSE 0 END) AS total_bot_comments,
    SUM(CASE WHEN author_type = 'user' THEN 1 ELSE 0 END) AS total_user_comments,
    SUM(CASE WHEN author_type = 'admin' THEN 1 ELSE 0 END) AS total_admin_comments,
    COUNT(*) AS total_comments
  FROM INTERCOM.CONVERSATION_PART_HISTORY
  WHERE part_type = 'comment'
  GROUP BY 1
),

first_admin_response AS (
SELECT 
  conversation_id, 
  created_at AS first_admin_response_timestamp
FROM CHIPPER.INTERCOM.CONVERSATION_PART_HISTORY
WHERE AUTHOR_TYPE = 'admin'
QUALIFY ROW_NUMBER() OVER (PARTITION BY CONVERSATION_ID ORDER BY CREATED_AT) = '1'
),

ticket_close_time AS (
SELECT 
  conversation_id, 
  created_at AS ticket_close_timestamp
FROM CHIPPER.INTERCOM.CONVERSATION_PART_HISTORY
WHERE PART_TYPE = 'close'
QUALIFY ROW_NUMBER() OVER (PARTITION BY CONVERSATION_ID ORDER BY CREATED_AT) = '1'
),

main AS (
  SELECT
      latest_tickets.id,
      latest_tickets.updated_at,
      latest_tickets.created_at,
      latest_tickets.open,
      latest_tickets.state,
      latest_tickets.read,
      latest_tickets.waiting_since,
      latest_tickets.snoozed_until,
      latest_tickets.priority,
      latest_tickets.source_type,
      latest_tickets.source_id,
      latest_tickets.source_delivered_as,
      latest_tickets.source_subject,
      latest_tickets.source_body,
      latest_tickets.source_url,
      latest_tickets.source_author_type,
      latest_tickets.source_author_id,
      latest_tickets.assignee_id,
      latest_tickets.assignee_type,
      latest_tickets.first_contact_reply_type,
      latest_tickets.first_contact_reply_url,
      latest_tickets.first_contact_reply_created_at,
      latest_tickets.conversation_rating_remark,
      latest_tickets.conversation_rating_created_at,
      latest_tickets.conversation_rating_value,
      latest_contact_history.location_country,
      latest_contact_history.location_region,
      latest_contact_history.external_id AS user_id,
      latest_contact_history.custom_primary_currency AS primary_currency,
      CASE 
          WHEN assignee_type = 'team' THEN team.name 
          WHEN assignee_type = 'bot' THEN 'BOT'
          WHEN assignee_type = 'admin' THEN admin_teams.teams
          ELSE NULL
      END AS team,
      CASE 
          WHEN assignee_type = 'team' THEN team_groups.team_group 
          WHEN assignee_type = 'bot' THEN 'BOT'
          WHEN assignee_type = 'admin' THEN admin_teams.team_groups
          ELSE NULL
      END AS team_group,
      CASE 
          WHEN assignee_type = 'admin' THEN admin_teams.name
          ELSE NULL
      END AS admin_name,
      number_of_comments.total_bot_comments,
      number_of_comments.total_user_comments,
      number_of_comments.total_admin_comments,
      number_of_comments.total_comments,
      tag.name AS tag,
      (CASE
        WHEN (UPPER(tag.name) LIKE '%AC-%' OR UPPER(tag.name) LIKE '%AC -%' OR UPPER(tag.name) LIKE '%ACCOUNT%' OR UPPER(tag.name) LIKE '%PIN%') AND 
        (UPPER(tag.name) NOT LIKE '%STOCKS%' OR UPPER(tag.name) IS NULL) THEN 'Account'
        WHEN (UPPER(tag.name) LIKE '%AIRTIME%') THEN 'Airtime'
        WHEN (UPPER(tag.name) LIKE '%BA-%' OR UPPER(tag.name) LIKE '%BA -%') THEN 'Bank Account'
        WHEN (UPPER(tag.name) LIKE '%BT -%') THEN 'Bank Transfers'
        WHEN (UPPER(tag.name) LIKE '%CHARGE%' OR UPPER(tag.name) LIKE '%CH -%') THEN 'Card Charge'
        WHEN (UPPER(tag.name) LIKE '%CRYPTO%') THEN 'Crypto'
        WHEN (UPPER(tag.name) LIKE '%NUBAN%') THEN 'Nuban'
        WHEN (UPPER(tag.name) LIKE '%P2P%' OR UPPER(tag.name) LIKE '%PAYMENTS%') AND (UPPER(tag.name) NOT LIKE '%STOCKS%' OR UPPER(tag.name) IS NULL) THEN 'P2P'
        WHEN (UPPER(tag.name) LIKE '%MARKETPLACE -%') THEN 'Marketplace'
        WHEN (UPPER(tag.name) LIKE '%REFERRAL%') THEN 'Referrals'
        WHEN (UPPER(tag.name) LIKE '%PAYOUT%') AND (UPPER(tag.name) NOT LIKE '%VIEW%' OR UPPER(tag.name) IS NULL) THEN 'Payouts'
        WHEN (UPPER(tag.name) LIKE '%S2NC%') THEN 'S2NC'
        WHEN (UPPER(tag.name) LIKE '%VIEW%') AND (UPPER(tag.name) NOT LIKE '%LRC%' OR UPPER(tag.name) IS NULL) THEN 'View'
        WHEN (UPPER(tag.name) LIKE '%LRC%') THEN 'LRC'
        WHEN (UPPER(tag.name) LIKE '%VIRTUAL CARD%') AND (UPPER(tag.name) NOT LIKE '%VIEW%' OR UPPER(tag.name) IS NULL) THEN 'Virtual Card'
        WHEN (UPPER(tag.name) LIKE '%STOCK%') THEN 'Stocks'
        ELSE 'Other'
      END) AS tag_group,

      first_admin_response.first_admin_response_timestamp,
      ticket_close_time.ticket_close_timestamp,
      DATEDIFF('minute',created_at,first_admin_response_timestamp) AS first_response_time,
      DATEDIFF('minute',created_at,ticket_close_timestamp) as time_to_close

  FROM LATEST_TICKETS
      LEFT JOIN LATEST_CONVERSATION_CONTACT_HISTORY 
      ON latest_conversation_contact_history.conversation_id = latest_tickets.id
      LEFT JOIN LATEST_CONTACT_HISTORY 
      ON latest_contact_history.id = latest_conversation_contact_history.contact_id
      LEFT JOIN CHIPPER.INTERCOM.TEAM 
      ON team.id = latest_tickets.assignee_id
      LEFT JOIN ADMIN_TEAMS 
      ON admin_teams.id = latest_tickets.assignee_id
      LEFT JOIN CONVERSATION_TAG_ID 
      ON conversation_tag_id.conversation_id =  latest_tickets.id
      LEFT JOIN "CHIPPER"."INTERCOM"."TAG" 
      ON tag.id = conversation_tag_id.tag_id
      LEFT JOIN NUMBER_OF_COMMENTS 
      ON number_of_comments.conversation_id = latest_tickets.id
      LEFT JOIN FIRST_ADMIN_RESPONSE 
      ON latest_tickets.id = FIRST_ADMIN_RESPONSE.conversation_id
      LEFT JOIN TICKET_CLOSE_TIME 
      ON latest_tickets.id = TICKET_CLOSE_TIME.conversation_id
      LEFT JOIN {{ref('intercom_team_groups')}} as team_groups 
      ON team.id = team_groups.team_id
      
)
SELECT *
FROM MAIN
