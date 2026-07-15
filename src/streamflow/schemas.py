from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID

from pyspark.sql.types import StringType, StructField, StructType
from pydantic import BaseModel, Field

class TransactionType(str, Enum):
    deposit = "deposit"
    withdrawal = "withdrawal"
    transfer = "transfer"
    purchase = "purchase"
    fee_charge = "fee charge"
    reversal = "reversal"

class TransactionStatus(str, Enum):
    pending = "PENDING"
    posted = "POSTED"
    failed = "FAILED"
    reversed = "REVERSED"

class TransactionSource(str, Enum):
    mobile_app = "mobile app"
    web_banking = "web banking"
    atm = "atm"
    teller = "teller"
    pos = "point of sale" # debit/credit card swipes
    api = "api" # third-party or internal system-to-system transactions
    wire_transfer = "wire transfer"
    check = "check"
    third_party = "third party" # zelle, venmo, paypal, etc. 

class TransactionEvent(BaseModel):
    schema_version: str = "1.0"
    event_id: UUID
    event_type: TransactionType
    event_ts: datetime
    source: TransactionSource
    account_id: str = Field(min_length=1)
    amount: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    status: TransactionStatus

    model_config = {"extra": "forbid"}


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

ALLOWED_EVENT_TYPES = tuple(member.value for member in TransactionType)
ALLOWED_SOURCES = tuple(member.value for member in TransactionSource)
ALLOWED_STATUSES = tuple(member.value for member in TransactionStatus)

REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "event_ts",
    "source",
    "account_id",
    "amount",
    "status",
)

INVALID_REASONS = [
    "MISSING_REQUIRED_FIELD",
    "INVALID_EVENT_ID",
    "INVALID_EVENT_TS",
    "INVALID_AMOUNT",
    "INVALID_EVENT_TYPE",
    "INVALID_SOURCE",
    "INVALID_STATUS",
    "DUPLICATE_EVENT_ID",
]

UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
AMOUNT_PATTERN = r"^\d+(\.\d{2})$"


def main() -> None:
    import json

    event_schema = TransactionEvent.model_json_schema()
    print(json.dumps(event_schema, indent=2))


if __name__ == "__main__":
    main()