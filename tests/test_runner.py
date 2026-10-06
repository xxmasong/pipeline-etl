from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

import pytest

from pipeline.runner import PipelineRunner, PipelineSpec
from pipeline.validation import Validator, is_decimal, non_negative, required
from pipeline.watermark import InMemoryWatermarkStore, LoadWindow, WatermarkManager

T0 = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


class FakeLoader:
    def __init__(self, *, fail_on_upsert: bool = False) -> None:
        self.upserted: list[dict[str, Any]] = []
        self.quarantined: list[dict[str, Any]] = []
        self.statements: list[str] = []
        self._fail = fail_on_upsert

    def upsert(self, sql: str, rows: Sequence[dict[str, Any]]) -> int:
        if self._fail:
            raise RuntimeError("database unavailable")
        self.statements.append(sql)
        self.upserted.extend(rows)
        return len(rows)

    def quarantine(self, pipeline: str, rows: Sequence[dict[str, Any]]) -> int:
        self.quarantined.extend(rows)
        return len(rows)


def make_spec(rows: list[dict[str, Any]], *, threshold: float = 0.05) -> PipelineSpec:
    return PipelineSpec(
        name="sales",
        table="fact_sales",
        columns=["id", "amount"],
        key_columns=["id"],
        extractor=lambda _window: rows,
        validator=Validator(
            {"id": [required], "amount": [required, is_decimal, non_negative]},
            error_threshold=threshold,
        ),
    )


def make_runner(loader: FakeLoader, **kwargs) -> tuple[PipelineRunner, InMemoryWatermarkStore]:
    store = InMemoryWatermarkStore()
    return PipelineRunner(loader, WatermarkManager(store, **kwargs)), store


class TestHappyPath:
    def test_loads_valid_rows_and_commits(self):
        loader = FakeLoader()
        runner, store = make_runner(loader)
        spec = make_spec([{"id": "1", "amount": "10"}, {"id": "2", "amount": "20"}])

        report = runner.run(spec, now=T0)

        assert report.succeeded
        assert report.extracted == 2
        assert report.loaded == 2
        assert report.quarantined == 0
        assert report.committed
        assert store.get("sales") == T0

    def test_uses_an_upsert_so_reruns_are_idempotent(self):
        loader = FakeLoader()
        runner, _ = make_runner(loader)
        runner.run(make_spec([{"id": "1", "amount": "10"}]), now=T0)

        assert "ON CONFLICT (id) DO UPDATE SET" in loader.statements[0]

    def test_an_empty_window_is_a_no_op(self):
        loader = FakeLoader()
        runner, store = make_runner(loader, lookback=timedelta(0))
        store.set("sales", T0 + timedelta(days=1))

        report = runner.run(make_spec([{"id": "1", "amount": "10"}]), now=T0)

        assert report.window.is_empty
        assert report.extracted == 0
        assert loader.upserted == []

    def test_no_extracted_rows_still_commits(self):
        loader = FakeLoader()
        runner, store = make_runner(loader)

        report = runner.run(make_spec([]), now=T0)

        # Otherwise an idle period would make the window grow without bound.
        assert report.committed
        assert store.get("sales") == T0


class TestQuarantine:
    def test_bad_rows_are_quarantined_while_good_ones_load(self):
        rows = [{"id": str(i), "amount": "10"} for i in range(19)]
        rows.append({"id": "20", "amount": "-5"})

        loader = FakeLoader()
        runner, store = make_runner(loader)
        report = runner.run(make_spec(rows), now=T0)

        assert report.loaded == 19
        assert report.quarantined == 1
        assert report.committed
        assert store.get("sales") == T0

    def test_quarantined_rows_carry_their_reasons(self):
        rows = [{"id": str(i), "amount": "10"} for i in range(19)]
        rows.append({"id": "20", "amount": "oops"})

        loader = FakeLoader()
        runner, _ = make_runner(loader)
        runner.run(make_spec(rows), now=T0)

        assert "_reasons" in loader.quarantined[0]
        assert "amount" in loader.quarantined[0]["_reasons"]


class TestFailureHandling:
    def test_too_many_bad_rows_aborts_without_committing(self):
        rows = [{"id": "1", "amount": "10"}, {"id": None, "amount": "10"}]

        loader = FakeLoader()
        runner, store = make_runner(loader)
        report = runner.run(make_spec(rows, threshold=0.05), now=T0)

        assert not report.succeeded
        assert not report.committed
        # The window must be retried once the feed is fixed.
        assert store.get("sales") is None
        assert loader.upserted == []

    def test_a_load_failure_leaves_the_watermark_untouched(self):
        loader = FakeLoader(fail_on_upsert=True)
        runner, store = make_runner(loader)

        report = runner.run(make_spec([{"id": "1", "amount": "10"}]), now=T0)

        assert not report.succeeded
        assert "database unavailable" in report.error
        assert store.get("sales") is None

    def test_the_next_run_reprocesses_after_a_failure(self):
        loader = FakeLoader(fail_on_upsert=True)
        runner, store = make_runner(loader, lookback=timedelta(0))
        store.set("sales", T0)

        first = runner.run(make_spec([{"id": "1", "amount": "10"}]), now=T0 + timedelta(hours=1))
        second = runner.run(make_spec([{"id": "1", "amount": "10"}]), now=T0 + timedelta(hours=2))

        assert first.window.start == second.window.start

    def test_an_extractor_error_is_reported_not_raised(self):
        def explode(_window: LoadWindow):
            raise ConnectionError("source unreachable")

        loader = FakeLoader()
        runner, store = make_runner(loader)
        spec = make_spec([])
        spec.extractor = explode

        report = runner.run(spec, now=T0)

        assert not report.succeeded
        assert "ConnectionError" in report.error
        assert store.get("sales") is None


class TestIncrementalBehaviour:
    def test_consecutive_runs_advance_the_window(self):
        loader = FakeLoader()
        runner, _ = make_runner(loader, lookback=timedelta(minutes=5))

        first = runner.run(make_spec([{"id": "1", "amount": "10"}]), now=T0)
        second = runner.run(
            make_spec([{"id": "2", "amount": "20"}]), now=T0 + timedelta(hours=1)
        )

        assert second.window.start == first.window.end - timedelta(minutes=5)
        assert second.window.end == T0 + timedelta(hours=1)

    def test_the_overlap_relies_on_the_upsert_for_correctness(self):
        loader = FakeLoader()
        runner, _ = make_runner(loader, lookback=timedelta(minutes=5))
        row = {"id": "1", "amount": "10"}

        runner.run(make_spec([row]), now=T0)
        runner.run(make_spec([row]), now=T0 + timedelta(minutes=1))

        # The same row is sent twice by design; ON CONFLICT makes that harmless.
        assert len(loader.upserted) == 2
        assert all("ON CONFLICT" in sql for sql in loader.statements)


def test_report_exposes_a_single_success_flag():
    loader = FakeLoader()
    runner, _ = make_runner(loader)
    report = runner.run(make_spec([{"id": "1", "amount": "10"}]), now=T0)
    assert report.succeeded is (report.error is None)


@pytest.mark.parametrize("threshold,expect_success", [(0.6, True), (0.4, False)])
def test_threshold_decides_whether_the_run_survives(threshold, expect_success):
    rows = [{"id": "1", "amount": "10"}, {"id": None, "amount": "10"}]
    loader = FakeLoader()
    runner, _ = make_runner(loader)

    report = runner.run(make_spec(rows, threshold=threshold), now=T0)
    assert report.succeeded is expect_success
