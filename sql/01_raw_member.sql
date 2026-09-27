-- SkyPoints: RAW member feed
-- Purpose:
--   Preserve the source member feed exactly as received, together with
--   ingestion/batch lineage.
--
-- Design:
--   RAW is source evidence, not a transformation layer.
--   The complete source line is preserved in RAW_RECORD.
--   Parsing, schema validation, type conversion, and business DQ happen
--   downstream in STAGING.
--
-- Source-contract note:
--   The assessment field specification lists an optional Post Code field,
--   but the supplied H/D source feed does not contain a Post Code field.
--   RAW therefore preserves the actual supplied source shape and does not
--   introduce a positional field that could shift DOB or Is_Active.
--
-- Schema evolution:
--   SOURCE_FIELD_COUNT is retained as metadata for detecting changes in
--   the source contract. A changed field count must be detected and
--   validated before the record is interpreted downstream.

CREATE TABLE IF NOT EXISTS SKYPOINTS.RAW.RAW_MEMBER_FEED (
    record_type              VARCHAR(1),

    -- Exact source line. This is the authoritative RAW representation.
    raw_record               VARCHAR(16777216),

    -- Number of business fields after the record-type indicator.
    -- Populated/validated downstream from RAW_RECORD.
    source_field_count       NUMBER(3,0),

    -- Batch and ingestion lineage
    batch_id                 VARCHAR(100),
    source_file_name         VARCHAR(500),
    source_feed_timestamp    TIMESTAMP_NTZ,
    ingestion_timestamp      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_row_number        NUMBER(38,0)
);


-- Raw-line file format
--
-- The source feed itself is pipe-delimited, but we intentionally use TAB
-- as the Snowflake parsing delimiter here so that the complete source line
-- is loaded into $1 without interpreting its internal pipe delimiters.
CREATE FILE FORMAT IF NOT EXISTS SKYPOINTS.RAW.MEMBER_RAW_LINE_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = '\t'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = FALSE;