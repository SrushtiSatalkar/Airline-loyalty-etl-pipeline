EXECUTE IMMEDIATE $$
DECLARE
    batch_id STRING DEFAULT '20260927';
    countries ARRAY DEFAULT ARRAY_CONSTRUCT(
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
    );
    c_code STRING;
BEGIN

    -- ========================================================
    -- 1. Create country target tables
    -- ========================================================

    FOR i IN 0 TO ARRAY_SIZE(countries) - 1 DO

        c_code := countries[i]::STRING;

        EXECUTE IMMEDIATE
            'CREATE TABLE IF NOT EXISTS SKYPOINTS.CURATED.MEMBER_'
            || c_code
            || ' LIKE SKYPOINTS.CURATED.MEMBER_CURRENT';

    END FOR;


    -- ========================================================
    -- 2. Identify members processed by this batch
    --
    -- MEMBER_STAGING contains only DQ-valid records, so these
    -- are the members eligible for country routing.
    -- ========================================================

    CREATE OR REPLACE TEMP TABLE SKYPOINTS.CURATED.BATCH_MEMBER_CHANGES AS

    SELECT DISTINCT
        member_id

    FROM SKYPOINTS.STAGING.MEMBER_STAGING

    WHERE batch_id = :batch_id;


    -- ========================================================
    -- 3. Remove these members from every country target
    --
    -- This handles country movement:
    --
    -- USA -> IND
    -- CAN -> USA
    -- etc.
    --
    -- It also makes reruns idempotent because the members are
    -- removed before their current state is reinserted.
    -- ========================================================

    FOR i IN 0 TO ARRAY_SIZE(countries) - 1 DO

        c_code := countries[i]::STRING;

        EXECUTE IMMEDIATE
            'DELETE FROM SKYPOINTS.CURATED.MEMBER_'
            || c_code
            || ' WHERE member_id IN '
            || '(SELECT member_id FROM SKYPOINTS.CURATED.BATCH_MEMBER_CHANGES)';

    END FOR;


    -- ========================================================
    -- 4. Insert each changed member into its current country
    -- ========================================================

    FOR i IN 0 TO ARRAY_SIZE(countries) - 1 DO

        c_code := countries[i]::STRING;

        EXECUTE IMMEDIATE
            'INSERT INTO SKYPOINTS.CURATED.MEMBER_'
            || c_code
            || ' '
            || 'SELECT c.* '
            || 'FROM SKYPOINTS.CURATED.MEMBER_CURRENT c '
            || 'JOIN SKYPOINTS.CURATED.BATCH_MEMBER_CHANGES b '
            || 'ON c.member_id = b.member_id '
            || 'WHERE c.country = '''
            || c_code
            || '''';

    END FOR;

END;
$$;