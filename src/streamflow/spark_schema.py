from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


TRANSACTION_SPARK_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("source", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

ALLOWED_EVENT_TYPES = (
    "deposit",
    "withdrawal",
    "transfer",
    "purchase",
    "fee charge",
    "reversal",
)

ALLOWED_SOURCES = (
    "mobile app",
    "web banking",
    "atm",
    "teller",
    "point of sale",
    "api",
    "wire transfer",
    "check",
    "third party",
)

ALLOWED_STATUSES = (
    "PENDING",
    "POSTED",
    "FAILED",
    "REVERSED",
)

REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "event_ts",
    "source",
    "account_id",
    "amount",
    "status",
)

INVALID_REASONS = ["MISSING_REQUIRED_FIELD",
                   "INVALID_EVENT_ID",
                   "INVALID_EVENT_TS",
                   "INVALID_AMOUNT",
                   "INVALID_EVENT_TYPE",
                   "INVALID_SOURCE",
                   "INVALID_STATUS",
                   "DUPLICATE_EVENT_ID"
                   ]

UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
AMOUNT_PATTERN = r"^\d+(\.\d{2})$"
