"""Postgres loader.

Each write is one transaction: a partial batch is never visible, which is what
lets the runner treat "the load committed" as a single fact.
"""

from __future__ import annotations

from typing import Any, Sequence

import psycopg
from psycopg.types.json import Jsonb


class PostgresLoader:
    def __init__(self, dsn: str, *, quarantine_table: str = "etl_quarantine") -> None:
        self._dsn = dsn
        self._quarantine_table = quarantine_table

    def upsert(self, sql: str, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    def quarantine(self, pipeline: str, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        # The row is stored as JSON so a feed that changes shape still lands
        # somewhere inspectable rather than failing the insert too.
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    f"INSERT INTO {self._quarantine_table} (pipeline, row, reasons) "
                    f"VALUES (%s, %s, %s)",
                    [
                        (pipeline, Jsonb({k: v for k, v in r.items() if k != "_reasons"}),
                         r.get("_reasons", ""))
                        for r in rows
                    ],
                )
            conn.commit()
        return len(rows)


class PostgresWatermarkStore:
    """Persists watermarks in the warehouse itself, so they survive a redeploy."""

    def __init__(self, dsn: str, *, table: str = "etl_watermarks") -> None:
        self._dsn = dsn
        self._table = table

    def get(self, pipeline: str):
        with psycopg.connect(self._dsn) as conn:
            row = conn.execute(
                f"SELECT value FROM {self._table} WHERE pipeline = %s", (pipeline,)
            ).fetchone()
        return row[0] if row else None

    def set(self, pipeline: str, value) -> None:
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                f"INSERT INTO {self._table} (pipeline, value) VALUES (%s, %s) "
                f"ON CONFLICT (pipeline) DO UPDATE SET value = EXCLUDED.value",
                (pipeline, value),
            )
            conn.commit()
