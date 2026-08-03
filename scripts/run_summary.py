import sys
from pathlib import Path

import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.append(str(parent_dir))

from spark.jobs.daily_summary import (
    create_transaction_details,
    create_transaction_summary,
    write_summary,
    write_transaction_details,
)

def main():
    # Create the spark session
    spark = SparkSession.builder \
        .appName("LocalSummary") \
        .config("spark.pyspark.python", sys.executable) \
        .config("spark.pyspark.driver.python", sys.executable) \
        .getOrCreate()
    
    try:
        # Import only validated events for curated outputs.
        project_root = parent_dir
        new_data = spark.read.parquet(str(project_root / "data/valid/events"))

        # Create a DataFrame containing the exact transaction details
        details_df = create_transaction_details(new_data)
        target_event_date = os.environ.get("TARGET_EVENT_DATE")
        if target_event_date:
            details_df = details_df.filter(
                F.col("event_date") == F.to_date(F.lit(target_event_date))
            )

        write_transaction_details(details_df, output_path=str(project_root / "data/curated/transaction_details"))

        # Create a summary DataFrame and write it to a parquet file
        summary_df = create_transaction_summary(details_df)
        write_summary(summary_df, output_path=str(project_root / "data/curated/daily_summary"))

        # Changing the grouping and path for the status summary
        status_summary_df = create_transaction_summary(
            details_df,
            group=["event_date", "event_type", "status"],
        )
        
        write_summary(
            status_summary_df,
            output_path=str(project_root / "data/curated/status_summary"),
        )
    
    finally:
        # Stop the spark instance
        spark.stop()

if __name__ == "__main__":
    main()
