from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

# A function to let you properly run the code
def add_src_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    src_path = repo_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


add_src_to_path()

from streamflow.schemas import ( 
    TransactionEvent,
    TransactionSource,
    TransactionStatus,
    TransactionType,
)


VALID_EVENT_TYPES = list(TransactionType)
VALID_SOURCES = list(TransactionSource)
VALID_STATUSES = list(TransactionStatus)


def build_valid_event(index: int) -> TransactionEvent:
    return TransactionEvent(
        event_id=uuid4(),
        event_type=VALID_EVENT_TYPES[index % len(VALID_EVENT_TYPES)],
        event_ts=datetime.now(timezone.utc),
        source=VALID_SOURCES[index % len(VALID_SOURCES)],
        account_id=f"ACCT-{1000 + index}",
        amount=Decimal(f"{50 + (index * 13)}.00"),
        status=VALID_STATUSES[index % len(VALID_STATUSES)],
    )


def build_invalid_events() -> list[dict[str, object]]:
    return [
        {
            "event_id": str(uuid4()),
            "event_type": TransactionType.deposit.value,
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "source": TransactionSource.mobile_app.value,
            "account_id": "ACCT-2001",
            "amount": "invalid-amount",
            "status": TransactionStatus.posted.value,
        },
        {
            "event_id": str(uuid4()),
            "event_type": TransactionType.withdrawal.value,
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "source": TransactionSource.atm.value,
            "amount": "25.00",
            "status": TransactionStatus.pending.value,
        },
        {
            "event_id": str(uuid4()),
            "event_type": TransactionType.transfer.value,
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "source": TransactionSource.web_banking.value,
            "account_id": "ACCT-2003",
            "amount": "30.00",
            "status": "COMPLETE",
        },
        {
            "event_id": str(uuid4()),
            "event_type": TransactionType.purchase.value,
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "source": TransactionSource.pos.value,
            "account_id": "ACCT-2004",
            "amount": "12.50",
            "status": TransactionStatus.failed.value,
            "unexpected_field": "should trigger extra field validation",
        },
    ]


def main() -> None:
    for index in range(10):
        event = build_valid_event(index)
        print(json.dumps(event.model_dump(mode="json"), ensure_ascii=True))

    for event in build_invalid_events():
        print(json.dumps(event, ensure_ascii=True))


if __name__ == "__main__":
    main()
