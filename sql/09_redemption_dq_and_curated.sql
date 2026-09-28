-- SkyPoints: Redemption data quality and curated load

CREATE SCHEMA IF NOT EXISTS SKYPOINTS.STAGING;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS.CURATED;

CREATE TABLE IF NOT EXISTS SKYPOINTS.STAGING.REDEMPTION_DQ_QUARANTINE (
    batch_id               VARCHAR(100) NOT NULL,
    source_file_name       VARCHAR(500) NOT NULL,
    source_row_number      NUMBER(38,0) NOT NULL,
    member_id              VARCHAR(18),
    txn_id                 VARCHAR(100),
    txn_date               DATE,
    partner                VARCHAR(255),
    miles_redeemed         NUMBER(18,0),
    status                 VARCHAR(20),
    dq_reason              VARCHAR(1000) NOT NULL,
    ingestion_timestamp    TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS SKYPOINTS.CURATED.REDEMPTION (
    member_id              VARCHAR(18) NOT NULL,
    txn_id                 VARCHAR(100) NOT NULL,
    feed_date              DATE,
    txn_date               DATE,
    partner                VARCHAR(255),
    miles_redeemed         NUMBER(18,0),
    status                 VARCHAR(20),
    batch_id               VARCHAR(100) NOT NULL,
    source_file_name       VARCHAR(500) NOT NULL,
    source_row_number      NUMBER(38,0) NOT NULL,
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TEMP TABLE SKYPOINTS.STAGING.REDEMPTION_DQ_EVALUATED AS

WITH duplicate_check AS (
    SELECT
        r.*,
        COUNT(*) OVER (
            PARTITION BY r.batch_id, r.txn_id
        ) AS txn_id_count
    FROM SKYPOINTS.STAGING.REDEMPTION_STAGING r
    WHERE r.batch_id = $BATCH_ID
),

evaluated AS (
    SELECT
        d.*,

        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(

                IFF(
                    d.member_id IS NULL,
                    'MEMBER_ID_MISSING',
                    NULL
                ),

                IFF(
                    d.txn_id IS NULL,
                    'TXN_ID_MISSING',
                    NULL
                ),

                IFF(
                    d.txn_date IS NULL,
                    'TXN_DATE_INVALID_OR_MISSING',
                    NULL
                ),
                
                IFF(
                    d.feed_date IS NULL,
                    'FEED_DATE_INVALID_OR_MISSING',
                    NULL
                ),

                IFF(
                    d.miles_redeemed IS NULL
                    OR d.miles_redeemed < 0,
                    'MILES_REDEEMED_INVALID',
                    NULL
                ),

                IFF(
                    d.status IS NULL
                    OR d.status NOT IN (
                        'COMPLETED',
                        'PENDING',
                        'CANCELLED'
                    ),
                    'STATUS_INVALID',
                    NULL
                ),

                IFF(
                    d.partner IS NULL,
                    'PARTNER_MISSING',
                    NULL
                ),

                IFF(
                    d.txn_id_count > 1,
                    'DUPLICATE_TXN_ID',
                    NULL
                ),

                IFF(
                    d.member_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM SKYPOINTS.CURATED.MEMBER_CURRENT m
                        WHERE m.member_id = d.member_id
                    ),
                    'MEMBER_NOT_FOUND',
                    NULL
                )

            ),
            '; '
        ) AS dq_reason

    FROM duplicate_check d
)

SELECT
    *,
    IFF(
        NULLIF(dq_reason, '') IS NULL,
        TRUE,
        FALSE
    ) AS is_valid
FROM evaluated;


-- ============================================================
-- 1. Quarantine invalid redemption records
-- ============================================================

MERGE INTO SKYPOINTS.STAGING.REDEMPTION_DQ_QUARANTINE AS target

USING (
    SELECT
        batch_id,
        source_file_name,
        source_row_number,
        member_id,
        txn_id,
        txn_date,
        partner,
        miles_redeemed,
        status,
        dq_reason,
        ingestion_timestamp
    FROM SKYPOINTS.STAGING.REDEMPTION_DQ_EVALUATED
    WHERE is_valid = FALSE
) AS source

ON target.batch_id = source.batch_id
AND target.source_file_name = source.source_file_name
AND target.source_row_number = source.source_row_number

WHEN MATCHED THEN UPDATE SET
    target.member_id = source.member_id,
    target.txn_id = source.txn_id,
    target.txn_date = source.txn_date,
    target.partner = source.partner,
    target.miles_redeemed = source.miles_redeemed,
    target.status = source.status,
    target.dq_reason = source.dq_reason,
    target.ingestion_timestamp = source.ingestion_timestamp

WHEN NOT MATCHED THEN INSERT (
    batch_id,
    source_file_name,
    source_row_number,
    member_id,
    txn_id,
    txn_date,
    partner,
    miles_redeemed,
    status,
    dq_reason,
    ingestion_timestamp
)
VALUES (
    source.batch_id,
    source.source_file_name,
    source.source_row_number,
    source.member_id,
    source.txn_id,
    source.txn_date,
    source.partner,
    source.miles_redeemed,
    source.status,
    source.dq_reason,
    source.ingestion_timestamp
);


-- ============================================================
-- 2. Load valid transactions into curated
-- ============================================================

MERGE INTO SKYPOINTS.CURATED.REDEMPTION AS target

USING (
    SELECT
        batch_id,
        source_file_name,
        source_row_number,
        member_id,
        txn_id,
        feed_date,
        txn_date,
        partner,
        miles_redeemed,
        status
    FROM SKYPOINTS.STAGING.REDEMPTION_DQ_EVALUATED
    WHERE is_valid = TRUE
) AS source

ON target.txn_id = source.txn_id

WHEN MATCHED THEN UPDATE SET
    target.member_id = source.member_id,
    target.feed_date = source.feed_date,
    target.txn_date = source.txn_date,
    target.partner = source.partner,
    target.miles_redeemed = source.miles_redeemed,
    target.status = source.status,
    target.batch_id = source.batch_id,
    target.source_file_name = source.source_file_name,
    target.source_row_number = source.source_row_number

WHEN NOT MATCHED THEN INSERT (
    member_id,
    txn_id,
    feed_date,
    txn_date,
    partner,
    miles_redeemed,
    status,
    batch_id,
    source_file_name,
    source_row_number
)
VALUES (
    source.member_id,
    source.txn_id,
    source.feed_date,
    source.txn_date,
    source.partner,
    source.miles_redeemed,
    source.status,
    source.batch_id,
    source.source_file_name,
    source.source_row_number
);

