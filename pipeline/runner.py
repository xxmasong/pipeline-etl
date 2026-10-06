"""Pipeline orchestration: extract a window, validate, load, then commit.

The ordering matters and is the whole point of this module. The watermark moves
only after the load has committed, so an interruption anywhere earlier means the
next run reads the same window again — which is safe because the load upserts on
the primary key.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Protocol, Sequence

from pipeline.validation import ValidationError, Validator
from pipeline.watermark import LoadWindow, WatermarkManager, upsert_statement, utcnow

logger = logging.getLogger(__name__)


class Loader(Protocol):
    """Writes rows and quarantined rejects, each atomically."""

    def upsert(self, sql: str, rows: Sequence[dict[str, Any]]) -> int: ...
    def quarantine(self, pipeline: str, rows: Sequence[dict[str, Any]]) -> int: ...


Extractor = Callable[[LoadWindow], Iterable[dict[str, Any]]]


@dataclass
class RunReport:
    pipeline: str
    window: LoadWindow
    extracted: int = 0
    loaded: int = 0
    quarantined: int = 0
    committed: bool = False
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


@dataclass
class PipelineSpec:
    name: str
    table: str
    columns: list[str]
    key_columns: list[str]
    extractor: Extractor
    validator: Validator


class PipelineRunner:
    def __init__(
        self,
        loader: Loader,
        watermarks: WatermarkManager,
    ) -> None:
        self._loader = loader
        self._watermarks = watermarks

    def run(self, spec: PipelineSpec, now: datetime | None = None) -> RunReport:
        now = now or utcnow()
        window = self._watermarks.window(spec.name, now)
        report = RunReport(pipeline=spec.name, window=window)

        if window.is_empty:
            logger.info("%s: empty window, nothing to do", spec.name)
            report.committed = True
            return report

        try:
            rows = list(spec.extractor(window))
            report.extracted = len(rows)

            result = spec.validator.validate(rows)
            report.quarantined = len(result.rejected)

            if result.rejected:
                # Keep the reasons alongside the row so the rejection is
                # diagnosable without re-running the batch.
                self._loader.quarantine(
                    spec.name,
                    [
                        {**rejection.row, "_reasons": "; ".join(rejection.reasons)}
                        for rejection in result.rejected
                    ],
                )
                logger.warning(
                    "%s: quarantined %d of %d rows",
                    spec.name,
                    len(result.rejected),
                    result.total,
                )

            if result.valid:
                sql = upsert_statement(spec.table, spec.columns, spec.key_columns)
                report.loaded = self._loader.upsert(sql, result.valid)

            # Only now is it safe to advance: the data is durable.
            self._watermarks.commit(spec.name, window)
            report.committed = True
            logger.info(
                "%s: loaded %d, quarantined %d, watermark now %s",
                spec.name,
                report.loaded,
                report.quarantined,
                window.end.isoformat(),
            )

        except ValidationError as exc:
            # Too many bad rows means the feed itself changed. Leave the
            # watermark where it is so the window is retried after a fix.
            report.error = str(exc)
            logger.error("%s: aborted, %s", spec.name, exc)

        except Exception as exc:  # noqa: BLE001 - reported, then re-raised context
            report.error = f"{type(exc).__name__}: {exc}"
            logger.exception("%s: failed", spec.name)

        return report
