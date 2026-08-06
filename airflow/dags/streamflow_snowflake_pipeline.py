from datetime import datetime, timedelta
import sys
from pathlib import Path

import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from airflow import DAG

# Comment out one of the following depending on your Airflow version.
# from airflow.providers.standard.operators.bash import BashOperator # Use for 3.x versions
# from airflow.providers.standard.operators.empty import EmptyOperator # ^
from airflow.operators.bash import BashOperator # Use for 2.x versions
from airflow.operators.empty import EmptyOperator # ^

root_dir = Path("/opt/airflow")

DEFAULT_ARGS = {
    "owner": "revature_wilmington_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "snowflake_pipeline",
    default_args = DEFAULT_ARGS,
    description = "Pushes data through the medallion architecture and into the Snowflake database.",
    schedule = "@daily",
    start_date = datetime(2026, 1, 1),
    catchup = False,
) as dag:

    task_check_source = BashOperator(
        task_id = "check_source_files",
        bash_command = f"cd {root_dir} && ls -l data/raw/events/*.parquet || echo 'No new raw files found'",
        
    )

    task_load_bronze = BashOperator(
        task_id = "load_bronze",
        bash_command = f"cd {root_dir} && python scripts/load_bronze_to_snowflake.py",
        env = {
            "SNOWFLAKE_USER": "{{ conn.snowflake_default.login }}",
            "SNOWFLAKE_PASSWORD": "{{ conn.snowflake_default.password }}",
        },
    )

    task_build_silver = BashOperator(
        task_id = "build_silver",
        bash_command = f"cd {root_dir} && python scripts/load_silver_to_snowflake.py",
        env = {
            "SNOWFLAKE_USER": "{{ conn.snowflake_default.login }}",
            "SNOWFLAKE_PASSWORD": "{{ conn.snowflake_default.password }}",
        },
    )

    task_quality_checks = BashOperator(
        task_id = "quality_checks",
        bash_command = f"cd {root_dir} && python scripts/load_silver_to_snowflake.py --skip-upload --skip-merge",
        env = {
            "SNOWFLAKE_USER": "{{ conn.snowflake_default.login }}",
            "SNOWFLAKE_PASSWORD": "{{ conn.snowflake_default.password }}",
        },
    )

    task_build_gold = BashOperator(
        task_id = "build_gold",
        bash_command = f"cd {root_dir} && python scripts/load_gold_to_snowflake.py",
        env = {
            "SNOWFLAKE_USER": "{{ conn.snowflake_default.login }}",
            "SNOWFLAKE_PASSWORD": "{{ conn.snowflake_default.password }}",
        },
    )

    task_publish_run_summary = EmptyOperator(
        task_id = "publish_run_summary",
    )

    (
        task_check_source
        >> task_load_bronze
        >> task_build_silver
        >> task_quality_checks
        >> task_build_gold
        >> task_publish_run_summary
    )