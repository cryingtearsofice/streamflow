from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="streamflow_daily_summary",
    description="Triggers the daily transaction summary Spark job",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    run_daily_summary = BashOperator(
        task_id="run_daily_summary",
        bash_command="spark-submit /opt/airflow/spark/jobs/daily_summary.py",
    )
