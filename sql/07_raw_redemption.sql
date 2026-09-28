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

COPY INTO SKYPOINTS.RAW.RAW_REDEMPTION_FEED
(
    raw_record,
    batch_id,
    source_file_name,
    source_row_number
)
FROM (
    SELECT
        $1,
        '20260926',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @SKYPOINTS.RAW.AZURE_MEMBER_STAGE/redemption/20260926/
)
FILE_FORMAT = (
    FORMAT_NAME = 'SKYPOINTS.RAW.REDEMPTION_JSON_FORMAT'
)
PATTERN = '.*redemptions\.json'
ON_ERROR = 'ABORT_STATEMENT';