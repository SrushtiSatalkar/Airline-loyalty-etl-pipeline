-- SkyPoints: Flatten redemption JSON into relational staging

CREATE SCHEMA IF NOT EXISTS SKYPOINTS.STAGING;

CREATE TABLE IF NOT EXISTS SKYPOINTS.STAGING.REDEMPTION_STAGING (
    batch_id               VARCHAR(100) NOT NULL,
    source_file_name       VARCHAR(500) NOT NULL,
    source_row_number      NUMBER(38,0) NOT NULL,

    member_id              VARCHAR(18),
    feed_date              DATE,

    txn_id                 VARCHAR(100),
    txn_date               DATE,
    partner                VARCHAR(255),
    miles_redeemed         NUMBER(18,0),
    status                 VARCHAR(20),

    ingestion_timestamp    TIMESTAMP_NTZ
);

MERGE INTO SKYPOINTS.STAGING.REDEMPTION_STAGING AS target
USING (
    SELECT
        r.batch_id,
        r.source_file_name,
        r.source_row_number,

        r.raw_record:member_id::VARCHAR AS member_id,

        TRY_TO_DATE(
            r.raw_record:feed_date::VARCHAR,
            'YYYYMMDD'
        ) AS feed_date,

        redemption.value:txn_id::VARCHAR AS txn_id,

        TRY_TO_DATE(
            redemption.value:txn_date::VARCHAR,
            'YYYYMMDD'
        ) AS txn_date,

        redemption.value:partner::VARCHAR AS partner,

        TRY_TO_NUMBER(
            redemption.value:miles_redeemed::VARCHAR
        )::NUMBER(18,0) AS miles_redeemed,

        redemption.value:status::VARCHAR AS status,

        r.ingestion_timestamp

    FROM SKYPOINTS.RAW.RAW_REDEMPTION_FEED r,
    LATERAL FLATTEN(
        INPUT => r.raw_record:redemptions
    ) redemption

    WHERE r.batch_id = $BATCH_ID
) AS source

ON target.batch_id = source.batch_id
AND target.source_row_number = source.source_row_number
AND target.txn_id = source.txn_id

WHEN MATCHED THEN UPDATE SET
    target.member_id = source.member_id,
    target.feed_date = source.feed_date,
    target.txn_date = source.txn_date,
    target.partner = source.partner,
    target.miles_redeemed = source.miles_redeemed,
    target.status = source.status,
    target.ingestion_timestamp = source.ingestion_timestamp

WHEN NOT MATCHED THEN INSERT (
    batch_id,
    source_file_name,
    source_row_number,
    member_id,
    feed_date,
    txn_id,
    txn_date,
    partner,
    miles_redeemed,
    status,
    ingestion_timestamp
)
VALUES (
    source.batch_id,
    source.source_file_name,
    source.source_row_number,
    source.member_id,
    source.feed_date,
    source.txn_id,         
    source.txn_date,
    source.partner,
    source.miles_redeemed,
    source.status,
    source.ingestion_timestamp
);