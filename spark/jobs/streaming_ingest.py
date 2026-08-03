import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json

current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
src_dir = project_root / "src"
if str(src_dir) not in sys.path:
    sys.path.append(str(src_dir))

from streamflow.quality import apply_quality_rules
from streamflow.schemas import TRANSACTION_SPARK_SCHEMA


def start_ingestion():
    # Set Hadoop directory, directed here towards it being immediately within the main drive
    os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")

    # Create the Spark environment
    spark = SparkSession.builder \
        .appName("IngestionAndLogging") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # Create a Kafka dataframe via the Kafka stream
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "streamflow.events")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    raw_directory = project_root / "data" / "raw" / "events"
    reject_directory = project_root / "data" / "rejects" / "events"
    checkpoint_directory = project_root / "data" / "checkpoints" / "streaming_ingest"

    raw_directory.mkdir(parents=True, exist_ok=True)
    reject_directory.mkdir(parents=True, exist_ok=True)
    checkpoint_directory.mkdir(parents=True, exist_ok=True)

    parsed_events_df = kafka_df.select(
        from_json(col("value").cast("string"), TRANSACTION_SPARK_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    ).select("data.*", "kafka_timestamp", "kafka_partition", "kafka_offset")

    def process_microbatch(batch_df: DataFrame, batch_id: int) -> None:
        # Writing each micro-batch to a deterministic path allows safe replay without duplicates.
        valid_df, rejected_df = apply_quality_rules(batch_df)

        (valid_df.write
            .mode("overwrite")
            .parquet(str(raw_directory / f"batch_id={batch_id}")))

        (rejected_df.write
            .mode("overwrite")
            .parquet(str(reject_directory / f"batch_id={batch_id}")))

    # Open the writestream, writing raw data and checkpoints to their respective directories in the Parquet format
    query = parsed_events_df.writeStream \
        .foreachBatch(process_microbatch) \
        .option("checkpointLocation", str(checkpoint_directory)) \
        .start()

    # End the query
    query.awaitTermination()


if __name__ == "__main__":
    start_ingestion()
