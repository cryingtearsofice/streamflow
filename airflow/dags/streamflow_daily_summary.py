from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import sys
from pathlib import Path

current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.append(str(parent_dir))

DEFAULT_ARGS = {
    "owner": "revature_wilmington_team",
    "retries": 4,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id = "daily_summary",
    default_args = DEFAULT_ARGS,
    description = "Returns a daily summary of the jobs completed.",
    start_date = datetime(2026, 7, 14),
    schedule = '@daily',
    catchup = False,
    tags = ['summary', 'submit']
) as dag:
    submit_summary = SparkSubmitOperator(
        task_id="run_spark_submit_summary",
        conn_id="spark_default",
        application=f"{current_dir}/scripts/run_summary.py",
        verbose = True
    )

# IMPORTANT: with overwrite, this will replace all the files from when it has run previously. As is, there is no duplicate prevention.
# If the write functions were changed to append, this would have to change. Both ingested files and summary files to be split by date.
