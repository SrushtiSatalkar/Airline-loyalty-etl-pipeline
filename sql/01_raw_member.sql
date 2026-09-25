-- SkyPoints: RAW member profile table
-- Purpose: Preserve source-shaped member records and ingestion lineage.
-- Business transformations and DQ are handled downstream in STAGING.
-- Note: RAW preserves source evidence; it does not make business decisions.

CREATE TABLE IF NOT EXISTS SKYPOINTS.RAW.RAW_MEMBER (
    Member_Name            VARCHAR(255),
    Member_ID              VARCHAR(18),
    Enrollment_Date        VARCHAR(8),
    Last_Flight_Date       VARCHAR(8),
    Tier_Code              VARCHAR(5),
    Agent_Name             VARCHAR(255),
    State                  VARCHAR(5),
    Country                VARCHAR(5),
    Post_Code              NUMBER(5,0),
    DOB                    VARCHAR(8),
    Is_Active               VARCHAR(1),

    batch_id               VARCHAR(100),
    source_file_name       VARCHAR(500),
    source_feed_timestamp  TIMESTAMP_NTZ,
    ingestion_timestamp    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_row_number      NUMBER(38,0)
);