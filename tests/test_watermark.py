from datetime import datetime, timedelta, timezone

import pytest

from pipeline.watermark import (
    InMemoryWatermarkStore,
    WatermarkManager,
    upsert_statement,
)

T0 = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


def manager(**kwargs) -> tuple[WatermarkManager, InMemoryWatermarkStore]:
    store = InMemoryWatermarkStore()
    return WatermarkManager(store, **kwargs), store


class TestWindow:
    def test_first_run_defaults_to_one_day_back(self):
        mgr, _ = manager()
        window = mgr.window("sales", T0)
        assert window.start == T0 - timedelta(days=1)
        assert window.end == T0

    def test_first_run_honours_an_explicit_initial(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        mgr, _ = manager(initial=start)
        assert mgr.window("sales", T0).start == start

    def test_naive_datetime_is_rejected(self):
        mgr, _ = manager()
        with pytest.raises(ValueError, match="timezone-aware"):
            mgr.window("sales", datetime(2026, 3, 1, 12, 0))

    def test_subsequent_run_overlaps_by_the_lookback(self):
        mgr, store = manager(lookback=timedelta(minutes=5))
        store.set("sales", T0)

        window = mgr.window("sales", T0 + timedelta(hours=1))
        # Overlapping avoids losing rows that share the boundary timestamp.
        assert window.start == T0 - timedelta(minutes=5)

    def test_start_never_exceeds_end(self):
        mgr, store = manager(lookback=timedelta(0))
        store.set("sales", T0 + timedelta(days=1))

        window = mgr.window("sales", T0)
        assert window.start <= window.end
        assert window.is_empty

    def test_pipelines_are_tracked_separately(self):
        mgr, store = manager(lookback=timedelta(0))
        store.set("sales", T0)

        assert mgr.window("sales", T0 + timedelta(hours=1)).start == T0
        assert mgr.window("inventory", T0 + timedelta(hours=1)).start != T0


class TestCommit:
    def test_commit_advances_the_watermark(self):
        mgr, store = manager()
        window = mgr.window("sales", T0)
        mgr.commit("sales", window)
        assert store.get("sales") == T0

    def test_watermark_is_not_moved_backwards(self):
        mgr, store = manager()
        store.set("sales", T0)

        earlier = mgr.window("sales", T0 - timedelta(hours=1))
        mgr.commit("sales", earlier)

        # Rewinding would make every later run reprocess the gap.
        assert store.get("sales") == T0

    def test_a_failed_run_reprocesses_its_window(self):
        mgr, store = manager(lookback=timedelta(0))
        store.set("sales", T0)

        first = mgr.window("sales", T0 + timedelta(hours=1))
        # Simulate a crash: the window was read but never committed.
        second = mgr.window("sales", T0 + timedelta(hours=2))

        assert second.start == first.start

    def test_no_rows_still_advances_the_watermark(self):
        mgr, store = manager()
        window = mgr.window("sales", T0)
        mgr.commit("sales", window)
        assert store.get("sales") == window.end


class TestUpsertStatement:
    def test_builds_an_on_conflict_update(self):
        sql = upsert_statement("sales", ["id", "day", "amount"], ["id"])
        assert "INSERT INTO sales (id, day, amount)" in sql
        assert "VALUES (%(id)s, %(day)s, %(amount)s)" in sql
        assert "ON CONFLICT (id) DO UPDATE SET" in sql
        assert "day = EXCLUDED.day" in sql
        assert "amount = EXCLUDED.amount" in sql

    def test_key_columns_are_not_in_the_update_list(self):
        sql = upsert_statement("sales", ["id", "amount"], ["id"])
        assert "id = EXCLUDED.id" not in sql

    def test_composite_keys(self):
        sql = upsert_statement("sales", ["store", "day", "amount"], ["store", "day"])
        assert "ON CONFLICT (store, day)" in sql

    def test_all_columns_in_the_key_yields_do_nothing(self):
        sql = upsert_statement("link", ["a", "b"], ["a", "b"])
        assert "DO NOTHING" in sql
        assert "DO UPDATE" not in sql

    def test_rejects_empty_inputs(self):
        with pytest.raises(ValueError):
            upsert_statement("t", [], ["id"])
        with pytest.raises(ValueError):
            upsert_statement("t", ["id"], [])

    def test_rejects_a_key_not_present_in_columns(self):
        with pytest.raises(ValueError, match="missing"):
            upsert_statement("t", ["amount"], ["id"])
