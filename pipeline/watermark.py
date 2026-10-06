"""Watermark tracking for incremental, idempotent loads.

A pipeline that reloads everything on each run is simple but does not scale; one
that tracks "where did I get to" is cheap but easy to get subtly wrong. Two
mistakes are common enough to design against:

1. **Using a strict ``>`` comparison on a timestamp watermark.** Rows sharing the
   high-water timestamp are skipped forever, because the next run starts after it.
   Overlap by a small lookback and deduplicate on the primary key instead.

2. **Advancing the watermark before the load commits.** A crash between the two
   loses the batch permanently. The watermark advances only after a successful
   commit, so a failed run simply reprocesses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol


class WatermarkStore(Protocol):
    def get(self, pipeline: str) -> datetime | None: ...
    def set(self, pipeline: str, value: datetime) -> None: ...


@dataclass(frozen=True)
class LoadWindow:
    """The half-open interval ``[start, end)`` a run should read."""

    start: datetime
    end: datetime

    @property
    def is_empty(self) -> bool:
        return self.start >= self.end


class InMemoryWatermarkStore:
    """A store for tests and local runs."""

    def __init__(self) -> None:
        self._values: dict[str, datetime] = {}

    def get(self, pipeline: str) -> datetime | None:
        return self._values.get(pipeline)

    def set(self, pipeline: str, value: datetime) -> None:
        self._values[pipeline] = value


class WatermarkManager:
    def __init__(
        self,
        store: WatermarkStore,
        *,
        lookback: timedelta = timedelta(minutes=5),
        initial: datetime | None = None,
    ) -> None:
        self._store = store
        self._lookback = lookback
        self._initial = initial

    def window(self, pipeline: str, now: datetime) -> LoadWindow:
        """Compute the next window to read.

        The start overlaps the previous high-water mark by ``lookback`` so that
        rows written late, or sharing the boundary timestamp, are picked up again.
        Loads must therefore be idempotent — see ``upsert_statement``.
        """
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")

        last = self._store.get(pipeline)
        if last is None:
            start = self._initial if self._initial is not None else now - timedelta(days=1)
        else:
            start = last - self._lookback

        return LoadWindow(start=min(start, now), end=now)

    def commit(self, pipeline: str, window: LoadWindow) -> None:
        """Advance the watermark. Call this only after the load has committed."""
        current = self._store.get(pipeline)
        # Never move a watermark backwards: a re-run over an older window would
        # otherwise cause every later run to reprocess the gap.
        if current is not None and window.end <= current:
            return
        self._store.set(pipeline, window.end)


def upsert_statement(table: str, columns: list[str], key_columns: list[str]) -> str:
    """Build an idempotent INSERT ... ON CONFLICT DO UPDATE.

    Re-running a window must not duplicate rows, which is what makes the
    overlapping read window safe.
    """
    if not columns:
        raise ValueError("columns must not be empty")
    if not key_columns:
        raise ValueError("key_columns must not be empty")
    missing = set(key_columns) - set(columns)
    if missing:
        raise ValueError(f"key columns missing from columns: {sorted(missing)}")

    placeholders = ", ".join(f"%({c})s" for c in columns)
    updates = [c for c in columns if c not in key_columns]

    if updates:
        assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in updates)
        conflict = f"DO UPDATE SET {assignments}"
    else:
        # Every column is part of the key, so there is nothing to update.
        conflict = "DO NOTHING"

    return (
        f"INSERT INTO {table} ({', '.join(columns)})\n"
        f"VALUES ({placeholders})\n"
        f"ON CONFLICT ({', '.join(key_columns)}) {conflict}"
    )


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)
