"""A runnable demonstration, with no database required.

    python example.py

It loads a batch containing one bad row, shows that the good rows still land and
the bad one is quarantined, then runs again to show the overlapping window
re-sending rows harmlessly because the load upserts.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from pipeline.runner import PipelineRunner, PipelineSpec
from pipeline.validation import Validator, is_decimal, non_negative, one_of, required
from pipeline.watermark import InMemoryWatermarkStore, LoadWindow, WatermarkManager

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

START = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


class PrintingLoader:
    """Stands in for Postgres so the example runs anywhere."""

    def __init__(self) -> None:
        self.table: dict[str, dict[str, Any]] = {}
        self.quarantine_rows: list[dict[str, Any]] = []

    def upsert(self, sql: str, rows: Sequence[dict[str, Any]]) -> int:
        for row in rows:
            # The primary key is what makes a re-sent row harmless.
            self.table[row["id"]] = dict(row)
        return len(rows)

    def quarantine(self, pipeline: str, rows: Sequence[dict[str, Any]]) -> int:
        self.quarantine_rows.extend(rows)
        return len(rows)


def source(window: LoadWindow) -> list[dict[str, Any]]:
    """A fake upstream feed. One row is deliberately malformed."""
    rows = [
        {"id": f"s-{i}", "store_id": "manila-1", "amount": f"{100 + i}.00", "currency": "php"}
        for i in range(1, 20)
    ]
    rows.append({"id": "s-bad", "store_id": "manila-1", "amount": "-12.00", "currency": "php"})
    return rows


def main() -> None:
    loader = PrintingLoader()
    store = InMemoryWatermarkStore()
    runner = PipelineRunner(loader, WatermarkManager(store, lookback=timedelta(minutes=5)))

    spec = PipelineSpec(
        name="sales",
        table="fact_sales",
        columns=["id", "store_id", "amount", "currency"],
        key_columns=["id"],
        extractor=source,
        validator=Validator(
            {
                "id": [required],
                "store_id": [required],
                "amount": [required, is_decimal, non_negative],
                "currency": [required, one_of(["php", "usd"])],
            },
            error_threshold=0.10,
        ),
    )

    print("\n--- first run ---")
    first = runner.run(spec, now=START)
    print(f"extracted={first.extracted} loaded={first.loaded} quarantined={first.quarantined}")
    print(f"committed={first.committed} watermark={store.get('sales')}")
    for row in loader.quarantine_rows:
        print(f"  quarantined {row['id']}: {row['_reasons']}")

    print("\n--- second run, one hour later ---")
    second = runner.run(spec, now=START + timedelta(hours=1))
    print(f"window starts {second.window.start.isoformat()} (overlaps by the lookback)")
    print(f"extracted={second.extracted} loaded={second.loaded}")
    print(f"rows in the target table: {len(loader.table)} (not {first.loaded + second.loaded})")
    print("\nThe overlap re-sent every row; ON CONFLICT made it a no-op.")


if __name__ == "__main__":
    main()
