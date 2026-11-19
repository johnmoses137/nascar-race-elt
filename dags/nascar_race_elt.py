from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

INCLUDE_DIR = "/usr/local/airflow/include"

with DAG(
    dag_id="nascar_race_elt",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command=f"python {INCLUDE_DIR}/extract.py",
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"python {INCLUDE_DIR}/load_bronze.py",
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=f"python {INCLUDE_DIR}/run_sql.py build_silver.sql",
    )

    check_quality = BashOperator(
        task_id="check_quality",
        bash_command=f"python {INCLUDE_DIR}/run_sql.py check_quality.sql --check",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"python {INCLUDE_DIR}/run_sql.py build_gold.sql",
    )

    # Each task waits for the previous task to succeed.
    extract >> load_bronze >> build_silver >> check_quality >> build_gold