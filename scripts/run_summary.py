import sys
from pathlib import Path

import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.append(str(parent_dir))

from spark.jobs.daily_summary import create_transaction_details, create_transaction_summary, write_summary, write_transaction_details

def main():
    # Create the spark session
    spark = SparkSession.builder \
        .appName("LocalSummary") \
        .config("spark.pyspark.python", sys.executable) \
        .config("spark.pyspark.driver.python", sys.executable) \
        .getOrCreate()
    
    try:
        # Import data from the raw folder
        project_root = parent_dir
        # new_data = spark.read.parquet(str(project_root / "data/raw"))
        mock_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("event_ts", StringType(), True), # to_timestamp_ntz converts this string
            StructField("event_type", StringType(), True),
            StructField("source", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True)
        ])

        mock_rows = [
            ("1", "acc_101", "2026-07-14 10:00:00", "deposit", "web", 150.00, "completed"),
            ("2", "acc_102", "2026-07-14 11:30:00", "withdrawal", "mobile", 50.00, "completed")
        ]

        new_data = spark.createDataFrame(mock_rows, schema=mock_schema)

        # Create a DataFrame containing the exact transaction details
        details_df = create_transaction_details(new_data)
        write_transaction_details(details_df, output_path=str(project_root / "data/curated/transaction_details"))

        # Create a summary DataFrame and write it to a parquet file
        summary_df = create_transaction_summary(details_df)
        write_summary(summary_df, output_path=str(project_root / "data/curated/daily_summary"))
    
    finally:
        # Stop the spark instance
        spark.stop()

if __name__ == "__main__":
    main()
