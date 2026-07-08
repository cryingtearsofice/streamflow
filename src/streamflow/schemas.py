from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType
)

BANKING_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("source", StringType(), True),
    StructField("entity_id", StringType(), True),

    StructField("payload",
        MapType(StringType(), StringType()),
        True
    )
])