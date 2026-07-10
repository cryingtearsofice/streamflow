from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType


def start_ingestion():
    # Set Hadoop directory, directed here towards it being immediately within the main drive
    import os
    os.environ["HADOOP_HOME"] = "C:\\hadoop" 

    # Create the Spark environment
    spark = SparkSession.builder \
        .appName("IngestionAndLogging") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:4.1.2") \
        .getOrCreate()

    # Set the directories for the raw data and checkpoints, pointing towards the current file then going up two directories and into the data folder
    working_directory = os.path.dirname(os.path.abspath(__file__))

    raw_directory = os.path.abspath(os.path.join(working_directory, "..", "..", "data", "raw", "events"))
    checkpoint_directory = os.path.abspath(os.path.join(working_directory, "..", "..", "data", "checkpoints"))

    # Declare the JSON schema that the Kafka input will be cast to the schema structure we agreed upon
    transaction_schema = StructType([
        StructField("schema_version", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("status", StringType(), True)            
    ])

    # Create a Kafka dataframe via the Kafka stream
    # kafka-queue-name is a placeholder
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "kafka-queue-name") \
        .load()
    
    # Process the Kafka dataframe into the JSON format, casting types and various information about the entry onto it
    processed_df = kafka_df.select(
        from_json(col("value").cast("string"), transaction_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    ).select("data.*", "kafka_timestamp", "kafka_partition", "kafka_offset")

    # Open the writestream, writing raw data and checkpoints to their respective directories in the Parquet format
    query = processed_df.writeStream \
        .format("parquet") \
        .option("path", raw_directory) \
        .option("checkpointLocation", checkpoint_directory) \
        .start()

    # End the query
    query.awaitTermination()
