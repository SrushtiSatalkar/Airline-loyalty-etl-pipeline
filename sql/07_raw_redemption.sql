-- SkyPoints: Raw redemption JSON ingestion

CREATE FILE FORMAT IF NOT EXISTS SKYPOINTS.RAW.REDEMPTION_JSON_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;

CREATE TABLE IF NOT EXISTS SKYPOINTS.RAW.RAW_REDEMPTION_FEED (
    raw_record             VARIANT,
    batch_id               VARCHAR(100) NOT NULL,
    source_file_name       VARCHAR(500) NOT NULL,
    source_feed_timestamp  TIMESTAMP_NTZ,
    ingestion_timestamp    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_row_number      NUMBER(38,0)
);

