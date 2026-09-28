-- SkyPoints: Pipeline control metadata

CREATE SCHEMA IF NOT EXISTS SKYPOINTS.CONTROL;

CREATE TABLE IF NOT EXISTS SKYPOINTS.CONTROL.BATCH_RUN (
    batch_id                VARCHAR(100) NOT NULL,
    feed_type               VARCHAR(50) NOT NULL,
    source_file_name        VARCHAR(500),
    source_feed_timestamp   TIMESTAMP_NTZ,
    file_size_bytes         NUMBER(38,0),
    file_checksum           VARCHAR(128),
    status                  VARCHAR(30) NOT NULL,
    started_at              TIMESTAMP_NTZ,
    completed_at            TIMESTAMP_NTZ,
    rows_loaded             NUMBER(38,0),
    rows_rejected           NUMBER(38,0),
    error_message           VARCHAR(4000),
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_BATCH_RUN
        PRIMARY KEY (batch_id, feed_type)
);