import os
from pathlib import Path

import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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


def main():
    test_file = PROJECT_ROOT / "tests/sql/test_member_pipeline.sql"

    if not test_file.exists():
        raise FileNotFoundError(
            f"Test file not found: {test_file}"
        )

    connection = get_snowflake_connection()

    try:
        print("Running SkyPoints automated tests...")

        failures = []

        with test_file.open("r", encoding="utf-8") as file:
            for statement_cursor in connection.execute_stream(file):
                results = statement_cursor.fetchall()

                for row in results:
                    result = str(row[0])

                    if result.startswith("FAIL"):
                        failures.append(result)
                        print(result)
                    else:
                        print(result)

        if failures:
            raise AssertionError(
                f"{len(failures)} automated test(s) failed."
            )

        print("All automated tests passed.")

    finally:
        connection.close()

if __name__ == "__main__":
    main()