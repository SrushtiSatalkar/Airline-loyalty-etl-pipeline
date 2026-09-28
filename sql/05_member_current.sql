-- SkyPoints: Maintain current member state
--
-- Creates the canonical current-state table and then loads
-- the latest valid member version for the current batch.

CREATE SCHEMA IF NOT EXISTS SKYPOINTS.CURATED;

CREATE TABLE IF NOT EXISTS SKYPOINTS.CURATED.MEMBER_CURRENT (
    member_id                 VARCHAR(18)      NOT NULL,

    member_name               VARCHAR(255),
    enrollment_date           DATE,
    last_flight_date          DATE,
    tier_code                 VARCHAR(5),
    agent_name                VARCHAR(255),
    state                     VARCHAR(5),
    country                   VARCHAR(5),
    dob                       DATE,
    is_active                 VARCHAR(1),

    age                       NUMBER(3,0),
    stale_member              BOOLEAN,

    source_feed_timestamp     TIMESTAMP_NTZ,
    batch_id                  VARCHAR(100),
    source_file_name          VARCHAR(500),
    source_row_number         NUMBER(38,0),

    created_at                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 1. Quarantine same-version conflicting Member_ID records
-- ============================================================

MERGE INTO SKYPOINTS.STAGING.DQ_QUARANTINE AS target

USING (
    WITH versioned AS (
        SELECT
            s.*,

            HASH(
                member_name,
                enrollment_date,
                last_flight_date,
                tier_code,
                agent_name,
                state,
                country,
                dob,
                is_active,
                age,
                stale_member
            ) AS record_hash

        FROM SKYPOINTS.STAGING.MEMBER_STAGING s
        WHERE s.batch_id = $BATCH_ID
          AND s.member_id IS NOT NULL
    ),

    conflicting_versions AS (
        SELECT
            member_id,
            source_feed_timestamp
        FROM versioned
        GROUP BY
            member_id,
            source_feed_timestamp
        HAVING COUNT(DISTINCT record_hash) > 1
    )

    SELECT
        v.batch_id,
        v.source_file_name,
        v.source_row_number,
        v.record_type,
        v.source_field_count,
        v.raw_record,
        'MEMBER_ID_SAME_VERSION_CONFLICT' AS dq_reason,
        v.source_feed_timestamp,
        v.ingestion_timestamp

    FROM versioned v

    INNER JOIN conflicting_versions c
        ON v.member_id = c.member_id
       AND v.source_feed_timestamp = c.source_feed_timestamp

) AS source

ON target.batch_id = source.batch_id
AND target.source_file_name = source.source_file_name
AND target.source_row_number = source.source_row_number
AND target.dq_reason = source.dq_reason

WHEN NOT MATCHED THEN INSERT (
    batch_id,
    source_file_name,
    source_row_number,
    record_type,
    source_field_count,
    raw_record,
    dq_reason,
    source_feed_timestamp,
    ingestion_timestamp
)

VALUES (
    source.batch_id,
    source.source_file_name,
    source.source_row_number,
    source.record_type,
    source.source_field_count,
    source.raw_record,
    source.dq_reason,
    source.source_feed_timestamp,
    source.ingestion_timestamp
);


-- ============================================================
-- 2. Maintain current member state
-- ============================================================

MERGE INTO SKYPOINTS.CURATED.MEMBER_CURRENT AS target

USING (
    WITH versioned AS (
        SELECT
            s.*,

            HASH(
                member_name,
                enrollment_date,
                last_flight_date,
                tier_code,
                agent_name,
                state,
                country,
                dob,
                is_active,
                age,
                stale_member
            ) AS record_hash

        FROM SKYPOINTS.STAGING.MEMBER_STAGING s
        WHERE s.batch_id = $BATCH_ID
          AND s.member_id IS NOT NULL
    ),

    conflicting_versions AS (
        SELECT
            member_id,
            source_feed_timestamp
        FROM versioned
        GROUP BY
            member_id,
            source_feed_timestamp
        HAVING COUNT(DISTINCT record_hash) > 1
    ),

    non_conflicting AS (
        SELECT v.*

        FROM versioned v

        LEFT JOIN conflicting_versions c
            ON v.member_id = c.member_id
           AND v.source_feed_timestamp = c.source_feed_timestamp

        WHERE c.member_id IS NULL
    ),

    latest_version AS (
        SELECT *
        FROM non_conflicting

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY member_id
            ORDER BY source_feed_timestamp DESC
        ) = 1
    )

    SELECT
        member_id,
        member_name,
        enrollment_date,
        last_flight_date,
        tier_code,
        agent_name,
        state,
        country,
        dob,
        is_active,
        age,
        stale_member,
        source_feed_timestamp,
        batch_id,
        source_file_name,
        source_row_number

    FROM latest_version

) AS source

ON target.member_id = source.member_id

WHEN MATCHED
     AND source.source_feed_timestamp > target.source_feed_timestamp

THEN UPDATE SET
    target.member_name             = source.member_name,
    target.enrollment_date         = source.enrollment_date,
    target.last_flight_date        = source.last_flight_date,
    target.tier_code               = source.tier_code,
    target.agent_name              = source.agent_name,
    target.state                   = source.state,
    target.country                 = source.country,
    target.dob                     = source.dob,
    target.is_active               = source.is_active,
    target.age                     = source.age,
    target.stale_member            = source.stale_member,
    target.source_feed_timestamp   = source.source_feed_timestamp,
    target.batch_id                = source.batch_id,
    target.source_file_name        = source.source_file_name,
    target.source_row_number       = source.source_row_number,
    target.updated_at              = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    member_id,
    member_name,
    enrollment_date,
    last_flight_date,
    tier_code,
    agent_name,
    state,
    country,
    dob,
    is_active,
    age,
    stale_member,
    source_feed_timestamp,
    batch_id,
    source_file_name,
    source_row_number
)

VALUES (
    source.member_id,
    source.member_name,
    source.enrollment_date,
    source.last_flight_date,
    source.tier_code,
    source.agent_name,
    source.state,
    source.country,
    source.dob,
    source.is_active,
    source.age,
    source.stale_member,
    source.source_feed_timestamp,
    source.batch_id,
    source.source_file_name,
    source.source_row_number
);