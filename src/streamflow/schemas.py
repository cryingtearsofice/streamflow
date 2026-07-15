import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID
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
    amount: Decimal = Field(max_digits=12, decimal_places=2)
    status: TransactionStatus

    model_config = {"extra": "forbid"}

event_schema = TransactionEvent.model_json_schema()  
print(json.dumps(event_schema, indent=2))