import sys
from pathlib import Path
from pyspark.sql import SparkSession

primary_dir = Path(__file__).resolve().parent.parent
if str(primary_dir) not in sys.path:
    sys.path.append(str(primary_dir))

from spark.jobs.daily_summary import create_transaction_details, create_transaction_summary, write_summary, write_transaction_details

def main():
    # Create the spark session
    spark = SparkSession.builder.appName("LocalSummary").getOrCreate()
    
    try:
        # Import data from the raw folder
        new_data = spark.read.parquet("data/raw")

        # Create a DataFrame containing the exact transaction details
        details_df = create_transaction_details(new_data)
        write_transaction_details(details_df)

        # Create a summary DataFrame and write it to a parquet file
        summary_df = create_transaction_summary(details_df)
        write_summary(summary_df)
    
    finally:
        # Stop the spark instance
        spark.stop()

if __name__ == "__main__":
    main()
