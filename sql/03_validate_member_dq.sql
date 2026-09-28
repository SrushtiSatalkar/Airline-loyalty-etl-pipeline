-- SkyPoints: Member parsing and business DQ evaluation
--
-- Purpose:
--   1. Parse detail records from RAW.
--   2. Evaluate all applicable business DQ rules.
--   3. Expose one evaluated row per source detail record.
--
-- No data is inserted by this view.

CREATE SCHEMA IF NOT EXISTS SKYPOINTS.STAGING;

CREATE OR REPLACE VIEW SKYPOINTS.STAGING.V_MEMBER_DQ_EVALUATED AS

WITH raw_records AS (
    SELECT
        batch_id,
        source_file_name,
        source_row_number,
        raw_record,
        ingestion_timestamp,
        source_feed_timestamp,
        SPLIT_PART(raw_record, '|', 2) AS record_type,

        (
            LENGTH(raw_record)
            - LENGTH(REPLACE(raw_record, '|', ''))
            - 1
        ) AS source_field_count

    FROM SKYPOINTS.RAW.RAW_MEMBER_FEED r
),

parsed_records AS (
    SELECT
        batch_id,
        source_file_name,
        source_row_number,
        raw_record,
        record_type,
        source_field_count,
        source_feed_timestamp,
        ingestion_timestamp,
        

        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 3)), '') AS member_name,
        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 4)), '') AS member_id,

        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 5)), '') AS enrollment_date_raw,
        TRY_TO_DATE(
            NULLIF(TRIM(SPLIT_PART(raw_record, '|', 5)), ''),
            'YYYYMMDD'
        ) AS enrollment_date,

        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 6)), '') AS last_flight_date_raw,
        TRY_TO_DATE(
            NULLIF(TRIM(SPLIT_PART(raw_record, '|', 6)), ''),
            'YYYYMMDD'
        ) AS last_flight_date,

        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 7)), '') AS tier_code,
        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 8)), '') AS agent_name,
        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 9)), '') AS state,
        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 10)), '') AS country,

        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 11)), '') AS dob_raw,
        TRY_TO_DATE(
            NULLIF(TRIM(SPLIT_PART(raw_record, '|', 11)), ''),
            'DDMMYYYY'
        ) AS dob,

        NULLIF(TRIM(SPLIT_PART(raw_record, '|', 12)), '') AS is_active

    FROM raw_records
    WHERE record_type = 'D'
),

dq_evaluated AS (
    SELECT
        p.*,

        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(

                IFF(source_field_count != 10,
                    'FIELD_COUNT_MISMATCH',
                    NULL),

                IFF(member_name IS NULL,
                    'MEMBER_NAME_MISSING',
                    NULL),

                IFF(member_id IS NULL,
                    'MEMBER_ID_MISSING',
                    NULL),

                IFF(enrollment_date_raw IS NULL,
                    'ENROLLMENT_DATE_MISSING',
                    NULL),

                IFF(
                    enrollment_date_raw IS NOT NULL
                    AND enrollment_date IS NULL,
                    'ENROLLMENT_DATE_INVALID',
                    NULL
                ),

                IFF(
                    last_flight_date_raw IS NOT NULL
                    AND last_flight_date IS NULL,
                    'LAST_FLIGHT_DATE_INVALID',
                    NULL
                ),

                IFF(
                    dob_raw IS NOT NULL
                    AND dob IS NULL,
                    'DOB_INVALID',
                    NULL
                ),

                IFF(country IS NULL,
                    'COUNTRY_MISSING',
                    NULL),

                IFF(
                    country IS NOT NULL
                    AND country NOT IN (
                        'USA',
                        'IND',
                        'PHIL',
                        'CAN',
                        'AU',
                        'CHN',
                        'BRA',
                        'GBR',
                        'JPN',
                        'ARE',
                        'FRA',
                        'DEU',
                        'SGP'
                    ),
                    'COUNTRY_UNSUPPORTED',
                    NULL
                ),

                IFF(
                    is_active IS NOT NULL
                    AND is_active NOT IN ('A', 'I'),
                    'IS_ACTIVE_INVALID',
                    NULL
                ),

                IFF(
                    LENGTH(member_name) > 255,
                    'MEMBER_NAME_TOO_LONG',
                    NULL
                ),

                IFF(
                    LENGTH(member_id) > 18,
                    'MEMBER_ID_TOO_LONG',
                    NULL
                ),

                IFF(
                    LENGTH(tier_code) > 5,
                    'TIER_CODE_TOO_LONG',
                    NULL
                ),

                IFF(
                    LENGTH(agent_name) > 255,
                    'AGENT_NAME_TOO_LONG',
                    NULL
                ),

                IFF(
                    LENGTH(state) > 5,
                    'STATE_TOO_LONG',
                    NULL
                ),

                IFF(
                    LENGTH(country) > 5,
                    'COUNTRY_TOO_LONG',
                    NULL
                )

            ),
            '; '
        ) AS dq_reason

    FROM parsed_records p
)

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

    NULLIF(dq_reason, '') AS dq_reason,

    IFF(
        NULLIF(dq_reason, '') IS NULL,
        TRUE,
        FALSE
    ) AS is_valid,
    source_feed_timestamp,
    ingestion_timestamp

FROM dq_evaluated;