-- SkyPoints: Member staging and data-quality quarantine
-- STAGING contains only validated, typed member records.
-- DQ_QUARANTINE contains records that cannot safely proceed downstream.

CREATE SCHEMA IF NOT EXISTS SKYPOINTS.STAGING;


-- ============================================================
-- 1. Validated member staging
-- Grain: one validated source D record per batch/file/row.
-- Multiple versions of the same Member_ID may exist across batches.
-- ============================================================

CREATE TABLE IF NOT EXISTS SKYPOINTS.STAGING.MEMBER_STAGING (
    batch_id                  VARCHAR(100)      NOT NULL,
    source_file_name          VARCHAR(500)      NOT NULL,
    source_row_number         NUMBER(38,0)      NOT NULL,

    record_type               VARCHAR(1)        NOT NULL,
    source_field_count        NUMBER(3,0)       NOT NULL,
    raw_record                VARCHAR(16777216) NOT NULL,

    member_name               VARCHAR(255),
    member_id                 VARCHAR(18),
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
    ingestion_timestamp       TIMESTAMP_NTZ
);


-- ============================================================
-- 2. Data-quality quarantine
-- Grain: one rejected source record per batch/file/row.
--
-- dq_reason contains all applicable validation failures for
-- that source record.
--
-- raw_record is retained so the rejected source can be
-- investigated and replayed after the issue is corrected.
-- ============================================================

CREATE TABLE IF NOT EXISTS SKYPOINTS.STAGING.DQ_QUARANTINE (
    batch_id                  VARCHAR(100)      NOT NULL,
    source_file_name          VARCHAR(500)      NOT NULL,
    source_row_number         NUMBER(38,0)      NOT NULL,

    record_type               VARCHAR(1),
    source_field_count        NUMBER(3,0),
    raw_record                VARCHAR(16777216) NOT NULL,

    dq_reason                 VARCHAR(1000)     NOT NULL,

    source_feed_timestamp     TIMESTAMP_NTZ,
    ingestion_timestamp       TIMESTAMP_NTZ
);