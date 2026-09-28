-- SkyPoints: Load validated member staging and DQ quarantine
--
-- Batch orchestration:
-- Relies on session variable $BATCH_ID set feed date during execution.

SET feed_as_of_date = TO_DATE($BATCH_ID, 'YYYYMMDD');
-- ============================================================
-- 1. Load valid member records
-- ============================================================

MERGE INTO SKYPOINTS.STAGING.MEMBER_STAGING AS target

USING (
    SELECT
        batch_id,
        source_file_name,
        source_row_number,

        record_type,
        source_field_count,
        raw_record,

        member_name,
        member_id,
        enrollment_date,
        last_flight_date,
        tier_code,
        agent_name,
        state,
        country,
        dob,
        is_active,

        DATEDIFF(
            year,
            dob,
            TO_DATE($feed_as_of_date)
        )
        -
        IFF(
            DATEADD(
                year,
                DATEDIFF(
                    year,
                    dob,
                    TO_DATE($feed_as_of_date)
                ),
                dob
            ) > TO_DATE($feed_as_of_date),
            1,
            0
        ) AS age,

        CASE
            WHEN last_flight_date IS NULL THEN NULL

            ELSE DATEDIFF(
                day,
                last_flight_date,
                TO_DATE($feed_as_of_date)
            ) > 90
        END AS stale_member,

        TO_TIMESTAMP_NTZ($feed_as_of_date) AS source_feed_timestamp,

        ingestion_timestamp

    FROM SKYPOINTS.STAGING.V_MEMBER_DQ_EVALUATED

    WHERE is_valid = TRUE
) AS source

ON  target.batch_id = source.batch_id
AND target.source_file_name = source.source_file_name
AND target.source_row_number = source.source_row_number

WHEN MATCHED THEN UPDATE SET
    target.record_type            = source.record_type,
    target.source_field_count     = source.source_field_count,
    target.raw_record             = source.raw_record,

    target.member_name            = source.member_name,
    target.member_id              = source.member_id,
    target.enrollment_date        = source.enrollment_date,
    target.last_flight_date       = source.last_flight_date,
    target.tier_code              = source.tier_code,
    target.agent_name             = source.agent_name,
    target.state                  = source.state,
    target.country                = source.country,
    target.dob                    = source.dob,
    target.is_active              = source.is_active,

    target.age                    = source.age,
    target.stale_member           = source.stale_member,

    target.source_feed_timestamp  = source.source_feed_timestamp,
    target.ingestion_timestamp    = source.ingestion_timestamp

WHEN NOT MATCHED THEN INSERT (
    batch_id,
    source_file_name,
    source_row_number,

    record_type,
    source_field_count,
    raw_record,

    member_name,
    member_id,
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
    ingestion_timestamp
)

VALUES (
    source.batch_id,
    source.source_file_name,
    source.source_row_number,

    source.record_type,
    source.source_field_count,
    source.raw_record,

    source.member_name,
    source.member_id,
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
    source.ingestion_timestamp
);


-- ============================================================
-- 2. Load rejected records into quarantine
-- ============================================================

MERGE INTO SKYPOINTS.STAGING.DQ_QUARANTINE AS target

USING (
    SELECT
        batch_id,
        source_file_name,
        source_row_number,

        record_type,
        source_field_count,
        raw_record,

        dq_reason,

        TO_TIMESTAMP_NTZ($feed_as_of_date)
            AS source_feed_timestamp,

        ingestion_timestamp

    FROM SKYPOINTS.STAGING.V_MEMBER_DQ_EVALUATED

    WHERE is_valid = FALSE
) AS source

ON  target.batch_id = source.batch_id
AND target.source_file_name = source.source_file_name
AND target.source_row_number = source.source_row_number

WHEN MATCHED THEN UPDATE SET
    target.record_type            = source.record_type,
    target.source_field_count     = source.source_field_count,
    target.raw_record             = source.raw_record,
    target.dq_reason              = source.dq_reason,
    target.source_feed_timestamp  = source.source_feed_timestamp,
    target.ingestion_timestamp    = source.ingestion_timestamp

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

