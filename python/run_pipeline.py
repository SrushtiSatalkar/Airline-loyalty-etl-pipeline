import argparse
import os
import re
from pathlib import Path
import yaml
import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_member_contract():
    contract_path = PROJECT_ROOT / "config/member_feed_schema.yml"

    if not contract_path.exists():
        raise FileNotFoundError(
            f"Member feed contract not found: {contract_path}"
        )

    with contract_path.open("r", encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    required_keys = [
        "version",
        "record_type",
        "expected_field_count",
        "expected_header",
        "fields",
    ]

    missing = [
        key for key in required_keys
        if key not in contract
    ]

    if missing:
        raise ValueError(
            "Member feed contract is missing: "
            + ", ".join(missing)
        )

    if len(contract["expected_header"]) != contract["expected_field_count"]:
        raise ValueError(
            "Contract error: expected_header length does not "
            "match expected_field_count."
        )

    if len(contract["fields"]) != contract["expected_field_count"]:
        raise ValueError(
            "Contract error: fields length does not "
            "match expected_field_count."
        )

    return contract

def validate_member_source_contract(connection, batch_id, contract):
    stage_path = (
        f"@SKYPOINTS.RAW.AZURE_MEMBER_STAGE/"
        f"member/{batch_id}/"
    )

    file_format = (
        "SKYPOINTS.RAW.MEMBER_RAW_LINE_FORMAT"
    )

    query = f"""
    SELECT $1
    FROM {stage_path}
    (
        FILE_FORMAT => '{file_format}'
    )
    LIMIT 1
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()

    if not result:
        raise RuntimeError(
            f"No member feed file found for batch {batch_id}."
        )

    header = result[0]

    parts = header.split("|")

    if len(parts) != contract["expected_field_count"] + 2:
        raise ValueError(
            "Member feed header field count does not match "
            "the source contract."
        )

    record_type = parts[1]

    if record_type != "H":
        raise ValueError(
            f"Expected header record type 'H', got '{record_type}'."
        )

    actual_header = parts[2:]

    if actual_header != contract["expected_header"]:
        raise ValueError(
            "Member feed header does not match the source contract.\n"
            f"Expected: {contract['expected_header']}\n"
            f"Received: {actual_header}"
        )

    print(
        f"Member source contract v{contract['version']}: PASS"
    )

def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the SkyPoints ETL pipeline."
    )

    parser.add_argument(
        "--batch-id",
        required=True,
        help="Batch ID in YYYYMMDD format, e.g. 20260926",
    )

    return parser.parse_args()


def validate_batch_id(batch_id):
    if not re.fullmatch(r"\d{8}", batch_id):
        raise ValueError(
            f"Invalid batch ID '{batch_id}'. Expected YYYYMMDD."
        )


def get_snowflake_connection():
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]

    missing = [
        variable
        for variable in required_vars
        if not os.getenv(variable)
    ]

    if missing:
        raise RuntimeError(
            "Missing Snowflake environment variables: "
            + ", ".join(missing)
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "SKYPOINTS"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

def start_batch_run(connection, batch_id):
    sql = """
    MERGE INTO SKYPOINTS.CONTROL.BATCH_RUN AS target
    USING (
        SELECT
            %s AS batch_id,
            'PIPELINE' AS feed_type
    ) AS source
    ON target.batch_id = source.batch_id
       AND target.feed_type = source.feed_type

    WHEN MATCHED THEN UPDATE SET
        status = 'RUNNING',
        started_at = CURRENT_TIMESTAMP(),
        completed_at = NULL,
        error_message = NULL

    WHEN NOT MATCHED THEN INSERT (
        batch_id,
        feed_type,
        status,
        started_at
    )
    VALUES (
        source.batch_id,
        source.feed_type,
        'RUNNING',
        CURRENT_TIMESTAMP()
    )
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (batch_id,))


def complete_batch_run(connection, batch_id, status, error_message=None):
    sql = """
    UPDATE SKYPOINTS.CONTROL.BATCH_RUN
    SET
        status = %s,
        completed_at = CURRENT_TIMESTAMP(),
        error_message = %s
    WHERE batch_id = %s
      AND feed_type = 'PIPELINE'
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (status, error_message, batch_id)
        )

def execute_sql_file(connection, sql_file):
    sql_path = PROJECT_ROOT / sql_file

    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path}"
        )

    print(f"Running {sql_file}...", end=" ")

    with sql_path.open("r", encoding="utf-8") as file:
        with connection.cursor() as cursor:
            for statement_cursor in connection.execute_stream(file):
                statement_cursor.fetchall()

    print("PASS")

def execute_copy(connection, copy_sql, feed_name):
    with connection.cursor() as cursor:
        cursor.execute(copy_sql)
        results = cursor.fetchall()

    if not results:
        print(f"Loading {feed_name}... SKIPPED")
        print("  No COPY result returned.")
        return

    # Snowflake COPY result:
    # file, status, rows_parsed, rows_loaded, errors_seen, ...
    first_result = results[0]

    if len(first_result) == 1 and isinstance(first_result[0], str):
        message = first_result[0]

        if "0 files processed" in message:
            print(f"Loading {feed_name}... SKIPPED")
            print("  No new files to load.")
            return

    files_loaded = sum(
        1
        for result in results
        if len(result) > 1 and result[1] == "LOADED"
    )

    rows_loaded = sum(
        result[3]
        for result in results
        if len(result) > 3 and result[1] == "LOADED"
    )

    print(f"Loading {feed_name}... PASS")
    print(f"  Files loaded: {files_loaded}")
    print(f"  Rows loaded: {rows_loaded}")

def load_member_raw(connection, batch_id):
    stage_path = f"@SKYPOINTS.RAW.AZURE_MEMBER_STAGE/member/{batch_id}/"

    copy_sql = f"""
    COPY INTO SKYPOINTS.RAW.RAW_MEMBER_FEED
    (
        record_type,
        raw_record,
        batch_id,
        source_file_name,
        source_feed_timestamp,
        source_row_number
    )
    FROM (
        SELECT
            SPLIT_PART($1, '|', 2),
            $1,
            '{batch_id}',
            METADATA$FILENAME,
            TO_TIMESTAMP_NTZ('{batch_id}', 'YYYYMMDD'),
            METADATA$FILE_ROW_NUMBER
        FROM {stage_path}
    )
    FILE_FORMAT = (
        FORMAT_NAME = 'SKYPOINTS.RAW.MEMBER_RAW_LINE_FORMAT'
    )
    PATTERN = '.*member_profile_feed\\.csv'
    ON_ERROR = 'ABORT_STATEMENT'
    """

    execute_copy(
    connection,
    copy_sql,
    "member RAW",
    )

def load_redemption_raw(connection, batch_id):
    stage_path = f"@SKYPOINTS.RAW.AZURE_MEMBER_STAGE/redemption/{batch_id}/"

    copy_sql = f"""
    COPY INTO SKYPOINTS.RAW.RAW_REDEMPTION_FEED
    (
        raw_record,
        batch_id,
        source_file_name,
        source_feed_timestamp,
        source_row_number
    )
    FROM (
        SELECT
            $1,
            '{batch_id}',
            METADATA$FILENAME,
            TO_TIMESTAMP_NTZ('{batch_id}', 'YYYYMMDD'),
            METADATA$FILE_ROW_NUMBER
        FROM {stage_path}
    )
    FILE_FORMAT = (
        FORMAT_NAME = 'SKYPOINTS.RAW.REDEMPTION_JSON_FORMAT'
    )
    PATTERN = '.*redemptions\\.json'
    ON_ERROR = 'ABORT_STATEMENT'
    """

    execute_copy(
    connection,
    copy_sql,
    "redemption RAW",
    )

def main():
    args = parse_args()

    validate_batch_id(args.batch_id)

    print("SkyPoints ETL Pipeline")
    print(f"Batch: {args.batch_id}")

    connection = get_snowflake_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SET BATCH_ID = %s",
                (args.batch_id,),
            )

            cursor.execute("SELECT $BATCH_ID")
            batch_id = cursor.fetchone()[0]

            print(f"Snowflake session batch: {batch_id}")
            print("Connection: PASS")

        start_batch_run(connection, args.batch_id)
        print("Batch tracking: RUNNING")

        execute_sql_file(
            connection,
            "sql/00_control.sql",
        )

        execute_sql_file(
            connection,
            "sql/01_raw_member.sql",
        )

        member_contract = load_member_contract()

        validate_member_source_contract(
            connection,
            args.batch_id,
            member_contract,
        )

        load_member_raw(
            connection,
            args.batch_id,
        )

        execute_sql_file(
            connection,
            "sql/07_raw_redemption.sql",
        )

        load_redemption_raw(
            connection,
            args.batch_id,
        )

        execute_sql_file(
            connection,
            "sql/03_validate_member_dq.sql",
        )

        execute_sql_file(
            connection,
            "sql/04_load_member_staging.sql",
        )

        execute_sql_file(
            connection,
            "sql/05_member_current.sql",
        )

        execute_sql_file(
            connection,
            "sql/06_country_targets.sql",
        )

        execute_sql_file(
            connection,
            "sql/08_staging_redemption.sql",
        )

        execute_sql_file(
            connection,
            "sql/09_redemption_dq_and_curated.sql",
        )
        complete_batch_run(connection, args.batch_id, "SUCCESS")
        print("Batch tracking: SUCCESS")
    
    except Exception as error:
        complete_batch_run(
            connection,
            args.batch_id,
            "FAILED",
            str(error)[:4000],
        )
        print("Batch tracking: FAILED")
        raise
    
    finally:
        connection.close()

if __name__ == "__main__":
    main()