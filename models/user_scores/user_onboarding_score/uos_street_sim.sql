SELECT
    STREET,
    COUNT(*) AS FREQ_STREET
FROM
    CHIPPER.{{var('compliance_public')}}.ADDRESSES
LEFT JOIN CHIPPER.{{var('core_public')}}.USERS
    ON ADDRESSES.USER_ID = USERS.ID
WHERE
    TIMESTAMPDIFF(HOUR, USERS.CREATED_AT, CURRENT_TIMESTAMP) <= 24
    AND STREET != ''
GROUP BY
    STREET
ORDER BY
    FREQ_STREET DESC
