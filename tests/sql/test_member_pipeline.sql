-- SkyPoints pipeline regression tests
-- These assertions validate core business and data-quality invariants.

-- Test 1: MEMBER_CURRENT contains at most one row per member_id

SELECT
    CASE
        WHEN COUNT(*) = 0
            THEN 'PASS: member_id uniqueness'
        ELSE 'FAIL: duplicate member_id found'
    END AS test_result
FROM (
    SELECT member_id
    FROM SKYPOINTS.CURATED.MEMBER_CURRENT
    GROUP BY member_id
    HAVING COUNT(*) > 1
);

-- Test 2: Every current member exists in exactly one country table

WITH country_members AS (
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_USA
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_IND
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_PHIL
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_CAN
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_AU
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_CHN
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_BRA
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_GBR
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_JPN
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_ARE
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_FRA
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_DEU
    UNION ALL
    SELECT member_id FROM SKYPOINTS.CURATED.MEMBER_SGP
),

country_violations AS (
    SELECT member_id
    FROM country_members
    GROUP BY member_id
    HAVING COUNT(*) <> 1

    UNION

    SELECT member_id
    FROM SKYPOINTS.CURATED.MEMBER_CURRENT
    WHERE member_id NOT IN (
        SELECT member_id
        FROM country_members
    )
)

SELECT
    CASE
        WHEN COUNT(*) = 0
            THEN 'PASS: country routing reconciliation'
        ELSE 'FAIL: country routing violation'
    END AS test_result,
    COUNT(*) AS violation_count
FROM country_violations;


-- Test 3: Newer source record must win for a member

SELECT
    CASE
        WHEN COUNT(*) = 1
            THEN 'PASS: latest record wins'
        ELSE 'FAIL: latest record did not win'
    END AS test_result,
    COUNT(*) AS matching_rows
FROM SKYPOINTS.CURATED.MEMBER_CURRENT
WHERE member_id = '223457'
  AND country = 'IND'
  AND source_feed_timestamp = '2026-09-27 00:00:00';


-- Test 4: Member country movement updates the country projection

WITH movement_check AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM SKYPOINTS.CURATED.MEMBER_IND
            WHERE member_id = '223457'
        ) AS in_new_country,

        (
            SELECT COUNT(*)
            FROM SKYPOINTS.CURATED.MEMBER_USA
            WHERE member_id = '223457'
        ) AS in_old_country
)

SELECT
    CASE
        WHEN in_new_country = 1
         AND in_old_country = 0
            THEN 'PASS: country movement'
        ELSE 'FAIL: country movement'
    END AS test_result,
    in_new_country,
    in_old_country
FROM movement_check;

-- Test 5: Curated redemption transactions are unique by txn_id

SELECT
    CASE
        WHEN COUNT(*) = 0
            THEN 'PASS: redemption txn_id uniqueness'
        ELSE 'FAIL: duplicate redemption txn_id found'
    END AS test_result,
    COUNT(*) AS duplicate_txn_ids
FROM (
    SELECT txn_id
    FROM SKYPOINTS.CURATED.REDEMPTION
    GROUP BY txn_id
    HAVING COUNT(*) > 1
);


-- Test 7: Every flattened redemption is either curated or quarantined

WITH counts AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM SKYPOINTS.STAGING.REDEMPTION_STAGING
            WHERE batch_id = '20260926'
        ) AS flattened_rows,

        (
            SELECT COUNT(*)
            FROM SKYPOINTS.CURATED.REDEMPTION
            WHERE batch_id = '20260926'
        ) AS curated_rows,

        (
            SELECT COUNT(*)
            FROM SKYPOINTS.STAGING.REDEMPTION_DQ_QUARANTINE
            WHERE batch_id = '20260926'
        ) AS quarantined_rows
)

SELECT
    CASE
        WHEN flattened_rows = curated_rows + quarantined_rows
            THEN 'PASS: redemption reconciliation'
        ELSE 'FAIL: redemption reconciliation'
    END AS test_result,
    flattened_rows,
    curated_rows,
    quarantined_rows
FROM counts;

-- Test 8: Pipeline rerun remains idempotent

WITH duplicate_curated AS (
    SELECT txn_id
    FROM SKYPOINTS.CURATED.REDEMPTION
    WHERE batch_id = '20260926'
    GROUP BY txn_id
    HAVING COUNT(*) > 1
),

duplicate_quarantine AS (
    SELECT
        batch_id,
        source_file_name,
        source_row_number
    FROM SKYPOINTS.STAGING.REDEMPTION_DQ_QUARANTINE
    WHERE batch_id = '20260926'
    GROUP BY
        batch_id,
        source_file_name,
        source_row_number
    HAVING COUNT(*) > 1
)

SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM duplicate_curated) = 0
         AND (SELECT COUNT(*) FROM duplicate_quarantine) = 0
            THEN 'PASS: redemption pipeline idempotency'
        ELSE 'FAIL: redemption pipeline is not idempotent'
    END AS test_result,
    (SELECT COUNT(*) FROM duplicate_curated) AS duplicate_curated,
    (SELECT COUNT(*) FROM duplicate_quarantine) AS duplicate_quarantine;