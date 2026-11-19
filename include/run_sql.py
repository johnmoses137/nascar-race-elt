import argparse
import os
from pathlib import Path

import snowflake.connector


def main():
    # Read the filename and whether this run is a quality check.
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    sql_file = Path(__file__).resolve().parent / "sql" / args.filename

    # Use the same Snowflake settings we tested inside Docker.
    with snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="SNOWFLAKE_JWT",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        private_key_file_pwd=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
    ) as connection:
        with sql_file.open(encoding="utf-8") as statements:
            for number, cursor in enumerate(connection.execute_stream(statements), start=1):
                try:
                    if args.check:
                        failure = cursor.fetchone()
                        if failure is not None:
                            raise RuntimeError(
                                f"{args.filename}: check {number} failed. "
                                f"First failing row: {failure}"
                            )
                    print(f"{args.filename}: statement {number} succeeded")
                finally:
                    cursor.close()


if __name__ == "__main__":
    main()