WITH account_flag AS (
    SELECT
        af.user_id,
        af.flagged_by,
        af.unflagged_by,
        af.flag_reason,
        af.unflag_reason,
        af.date_flagged,
        af.date_unflagged,
        af.flag,
        af.watchlist_match_id,
        RANK() OVER(PARTITION BY user_id ORDER BY date_flagged DESC) AS rnk
    FROM
        chipper.{{ var('compliance_public') }}.account_flags AS af
    WHERE
        _fivetran_deleted IS NULL
        AND unflagged_by IS NULL
)

SELECT
    account_flag.*,
    watchlist_matches.created_at AS wm_created_at,
    watchlist_matches.updated_at AS wm_updated_at,
    watchlist_matches.watchlist,
    watchlist_matches.full_response,
    watchlist_matches.status AS watchlist_status,
    watchlist_matches.match_type,
    u.tag
FROM
    account_flag
LEFT JOIN chipper.{{ var('compliance_public') }}.watchlist_matches
    ON watchlist_matches.id = account_flag.watchlist_match_id
LEFT JOIN chipper.{{ var('core_public') }}.users AS u
    ON account_flag.user_id = u.id
