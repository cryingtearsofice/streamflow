from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import sys
from pathlib import Path

current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.append(str(parent_dir))

from spark.jobs.daily_summary import create_transaction_details, create_transaction_summary, write_summary, write_transaction_details

DEFAULT_ARGS = {
    "owner": "revature_wilmington_team",
    "retries": 4,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id = "daily_summary",
    default_args = DEFAULT_ARGS,
    description = "Returns a daily summary of the jobs completed.",
    start_date = datetime(2026, 7, 14),
    schedule = '@daily',
    catchup = False,
    tags = ['summary', 'submit']
)

def streamflow_daily_summary():

    # Create the task containing the summary creation functions
    @task
    def process_daily_data():

        # Import additional modules
        # Notably, do not leave pyspark open the entire time
        from pyspark.sql import SparkSession
        from spark.jobs.daily_summary import (
            create_transaction_details, create_transaction_summary, 
            write_summary, write_transaction_details
        )

        # Create spark session inside of the task since we're running it locally, would not work on a cluster
        # To change it to a cluster, wrap the following in a script that runs its own Spark session
        spark = SparkSession.builder.appName("LocalSummary").getOrCreate()

        # Import data from the raw folder
        new_data = spark.read.parquet("data/raw")

        # Create a DataFrame containing the exact transaction details
        details_df = create_transaction_details(new_data)
        write_transaction_details(details_df)

        # Create a summary DataFrame and write it to a parquet file
        summary_df = create_transaction_summary(details_df)
        write_summary(summary_df)

        # Stop the spark instance
        spark.stop()

    # Call the task
    process_daily_data()

# IMPORTANT: with overwrite, this will replace all the files from when it has run previously. As is, there is no duplicate prevention.
# If the write functions were changed to append, this would have to change. Both ingested files and summary files to be split by date.
