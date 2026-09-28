-- SkyPoints: Maintain current member state
--
-- Loads the latest valid member version into MEMBER_CURRENT.
-- A source record replaces the current state only when its
-- source_feed_timestamp is newer than the existing record.
--
-- Batch orchestration:
-- Relies on session variable $BATCH_ID set during execution.

-- ============================================================
-- 1. Maintain current member state
-- ============================================================

MERGE INTO SKYPOINTS.CURATED.MEMBER_CURRENT AS target

USING (
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

    FROM SKYPOINTS.STAGING.MEMBER_STAGING

    WHERE batch_id = $BATCH_ID
) AS source

ON target.member_id = source.member_id

-- ============================================================
-- Existing member:
-- only a newer source version may replace current state.
-- ============================================================

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

-- ============================================================
-- New member:
-- no current state exists yet.
-- ============================================================

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