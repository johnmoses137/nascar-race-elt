import os
from getpass import getpass
import snowflake.connector
from pathlib import Path

raw_dir = Path(__file__).resolve().parent / "data" / "raw"

# Read the passphrase from the environment for unattended runs.
private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
if private_key_passphrase is None:
    private_key_passphrase = getpass("Private key passphrase: ")


with snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    authenticator="SNOWFLAKE_JWT",
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
    private_key_file_pwd=private_key_passphrase,
    warehouse="SYSTEM$STREAMLIT_NOTEBOOK_WH"

) as connection:

    with connection.cursor() as cursor:

        # Uploading local JSON files to Snowflake stage.
        cursor.execute(f"""
            PUT {raw_dir.as_uri()}/*.json
            @NASCAR_RACE_ELT.BRONZE.RAW_JSON_STAGE
        """)

        # Verifying files uploaded to the stage.
        cursor.execute("""
            LIST @NASCAR_RACE_ELT.BRONZE.RAW_JSON_STAGE
        """)

        print("Files on stage:", len(cursor.fetchall()))

        # Loading each JSON response with its filename and scan timestamp.
        cursor.execute("""
            COPY INTO NASCAR_RACE_ELT.BRONZE.RAW_NASCAR_JSON
                (RAW_DATA, SOURCE_FILE, LOADED_AT)
            FROM (
                SELECT $1, METADATA$FILENAME, METADATA$START_SCAN_TIME
                FROM @NASCAR_RACE_ELT.BRONZE.RAW_JSON_STAGE
            )
            FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
            ON_ERROR = ABORT_STATEMENT
        """)

        # Verify how many rows the Bronze table contains.
        cursor.execute("SELECT COUNT(*) FROM NASCAR_RACE_ELT.BRONZE.RAW_NASCAR_JSON")
        print("Rows in Bronze:", cursor.fetchone()[0])