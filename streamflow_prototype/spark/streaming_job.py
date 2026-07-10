from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pydantic import ValidationError


def add_src_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    src_path = repo_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


add_src_to_path()

from streamflow.schemas import TransactionEvent  # noqa: E402


def load_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            records.append(
                {
                    "line_number": line_number,
                    "original_record": line,
                    "validation_error": f"Invalid JSON: {exc}",
                }
            )
            continue
        records.append(record)
    return records


def validate_records(records: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    valid_records: list[dict[str, object]] = []
    invalid_records: list[dict[str, object]] = []

    for record in records:
        try:
            event = TransactionEvent.model_validate(record)
        except ValidationError as exc:
            invalid_records.append(
                {
                    "original_record": record,
                    "validation_error": str(exc),
                }
            )
            continue

        valid_records.append(event.model_dump(mode="json"))

    return valid_records, invalid_records


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate sample Streamflow events from a JSONL file.")
    parser.add_argument(
        "input_file",
        nargs="?",
        default="sample_events.jsonl",
        help="Path to the local JSONL file produced by the prototype producer.",
    )
    args = parser.parse_args()

    input_path = Path(args.input_file)
    records = load_jsonl(input_path)
    valid_records, invalid_records = validate_records(records)

    print(f"input_file: {input_path}")
    print(f"valid_records: {len(valid_records)}")
    print(f"invalid_records: {len(invalid_records)}")
    print(json.dumps({"valid_records": valid_records, "invalid_records": invalid_records}, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
