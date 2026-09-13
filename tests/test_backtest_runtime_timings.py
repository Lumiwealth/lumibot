"""Engine milestones must not confuse heartbeat, callback entry and data readiness."""

import csv
import json
from types import SimpleNamespace
from unittest.mock import Mock
from datetime import datetime, timedelta, timezone

import pytest

from tests.test_progress_logging import create_test_data_source


def test_callback_timestamp_survives_progress_and_is_not_simulation_advance(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKTESTING_PROGRESS_HEARTBEAT", "false")
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    source = create_test_data_source(str(tmp_path), start, start + timedelta(days=7))
    source.record_runtime_milestone("first_callback_entered_at")
    first = source.get_runtime_timings()["first_callback_entered_at"]
    source.record_runtime_milestone("first_callback_entered_at")
    assert source.get_runtime_timings()["first_callback_entered_at"] == first
    assert "first_simulation_advance_at" not in source.get_runtime_timings()
    source._update_datetime(source.datetime_start + timedelta(days=1))
    with open(source._progress_csv_path, newline="") as stream:
        row = next(csv.DictReader(stream))
    timings = json.loads(row["runtime_timings"])
    assert timings["first_callback_entered_at"] == first
    assert datetime.fromisoformat(first).utcoffset() == timedelta(0)
    assert "first_simulation_advance_at" in timings


def test_runtime_timings_are_per_source_bounded_and_return_copies(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKTESTING_PROGRESS_HEARTBEAT", "false")
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    first = create_test_data_source(str(tmp_path / "first"), start, start + timedelta(days=7))
    second = create_test_data_source(str(tmp_path / "second"), start, start + timedelta(days=7))
    first.record_runtime_milestone("first_callback_entered_at")
    first.record_runtime_milestone("untrusted-arbitrary-content")
    assert "first_callback_entered_at" not in second.get_runtime_timings()
    assert "untrusted-arbitrary-content" not in first.get_runtime_timings()
    snapshot = first.get_runtime_timings()
    snapshot.clear()
    assert "first_callback_entered_at" in first.get_runtime_timings()


def test_final_report_phase_is_published_without_advancing_simulation(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKTESTING_PROGRESS_HEARTBEAT", "false")
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    source = create_test_data_source(str(tmp_path), start, start + timedelta(days=7))
    source._update_datetime(source.datetime_start + timedelta(days=1))
    with open(source._progress_csv_path, newline="") as stream:
        before = next(csv.DictReader(stream))
    source.record_runtime_milestone("reports_completed_at")
    assert source.flush_runtime_timings()
    with open(source._progress_csv_path, newline="") as stream:
        after = next(csv.DictReader(stream))
    for field in ("percent", "simulation_date", "portfolio_value"):
        assert after[field] == before[field]
    assert "reports_completed_at" in json.loads(after["runtime_timings"])


def test_actual_callback_entry_is_recorded_before_callback_can_wait_for_prices(tmp_path, monkeypatch):
    from lumibot.strategies.strategy_executor import StrategyExecutor

    monkeypatch.setenv("BACKTESTING_PROGRESS_HEARTBEAT", "false")
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    source = create_test_data_source(str(tmp_path), start, start + timedelta(days=7))
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = SimpleNamespace(
        is_backtesting=True, sleeptime="1D", send_account_summary_to_discord=Mock(),
    )
    executor.broker = SimpleNamespace(
        IS_BACKTESTING_BROKER=True, data_source=source, is_market_open=lambda: True,
    )
    executor.sync_broker = Mock()
    executor._log_iteration_heartbeat = False
    executor._run_once_requested = True

    class CallbackReached(Exception):
        pass

    def callback():
        assert "first_callback_entered_at" in source.get_runtime_timings()
        assert "first_usable_price_at" not in source.get_runtime_timings()
        raise CallbackReached()

    executor._on_trading_iteration_callable = callback
    # Invoke the real lifecycle body; its decorators perform unrelated broker
    # valuation. The sentinel represents a callback awaiting its first price.
    with pytest.raises(CallbackReached):
        StrategyExecutor._on_trading_iteration.__wrapped__.__wrapped__(executor)


@pytest.mark.parametrize("price", [None, float("nan"), float("inf"), "invalid"])
def test_unusable_price_does_not_claim_readiness(price):
    from lumibot.strategies.strategy import Strategy

    recorder = Mock()
    strategy = SimpleNamespace(_record_backtest_runtime_milestone=recorder)
    Strategy._record_usable_backtest_price(strategy, price)
    recorder.assert_not_called()
