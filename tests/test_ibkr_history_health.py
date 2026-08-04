from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.tools.ibkr_history_health import (
    HistoryOutcome,
    classify_history_failure,
    coalesce_nearby_session_groups,
    group_contiguous_missing_sessions,
    ibkr_history_health_snapshot,
    padded_repair_window,
    record_history_health,
    reset_ibkr_history_health_for_testing,
    split_session_groups,
)


@pytest.fixture(autouse=True)
def _reset_history_health_state():
    reset_ibkr_history_health_for_testing()
    yield
    reset_ibkr_history_health_for_testing()


def test_history_failure_classification_never_persists_ambiguous_failures() -> None:
    malformed = classify_history_failure(
        RuntimeError("IBKR history remained invalid after rebuild: malformed_history_payload")
    )
    chart = classify_history_failure(RuntimeError("Chart data unavailable"))
    timeout = classify_history_failure(TimeoutError("queue timed out"))

    assert malformed.outcome is HistoryOutcome.PARTIAL
    assert malformed.persist_negative_cache is False
    assert chart.outcome is HistoryOutcome.TRANSIENT_FAILURE
    assert chart.identity_related is True
    assert chart.persist_negative_cache is False
    assert timeout.outcome is HistoryOutcome.TRANSIENT_FAILURE
    assert timeout.persist_negative_cache is False


def test_history_failure_classification_persists_only_confirmed_absence() -> None:
    classification = classify_history_failure(
        RuntimeError("Unable to resolve IBKR conid for DELISTED")
    )

    assert classification.outcome is HistoryOutcome.CONFIRMED_NO_DATA
    assert classification.persist_negative_cache is True


def test_missing_sessions_group_by_exchange_calendar_adjacency() -> None:
    expected = [
        pd.Timestamp("2025-04-28 16:00", tz="America/New_York"),
        pd.Timestamp("2025-04-29 16:00", tz="America/New_York"),
        pd.Timestamp("2025-04-30 16:00", tz="America/New_York"),
        pd.Timestamp("2025-05-01 16:00", tz="America/New_York"),
        pd.Timestamp("2025-05-02 16:00", tz="America/New_York"),
        pd.Timestamp("2025-05-05 16:00", tz="America/New_York"),
    ]

    groups = group_contiguous_missing_sessions(
        expected,
        [expected[1], expected[2], expected[5]],
    )

    assert [[value.date().isoformat() for value in group] for group in groups] == [
        ["2025-04-29", "2025-04-30"],
        ["2025-05-05"],
    ]

    coalesced = coalesce_nearby_session_groups(groups)
    assert [[value.date().isoformat() for value in group] for group in coalesced] == [
        ["2025-04-29", "2025-04-30", "2025-05-05"]
    ]


def test_padded_repair_window_stays_small() -> None:
    sessions = [
        pd.Timestamp("2025-04-29 16:00", tz="America/New_York"),
        pd.Timestamp("2025-04-30 16:00", tz="America/New_York"),
    ]

    start, end = padded_repair_window(sessions, padding_days=1)

    assert start.date().isoformat() == "2025-04-28"
    assert end.date().isoformat() == "2025-05-02"
    assert (end - start).days == 4


def test_large_gap_is_split_into_bounded_repair_segments() -> None:
    sessions = [
        pd.Timestamp("2025-01-02", tz="America/New_York") + pd.Timedelta(days=value)
        for value in range(25)
    ]

    groups = split_session_groups([sessions], max_sessions=10)

    assert [len(group) for group in groups] == [10, 10, 5]
    assert max((group[-1] - group[0]).days for group in groups) <= 9


def test_health_snapshot_is_bounded_and_contains_no_runtime_credentials() -> None:
    record_history_health(
        symbol="SQQQ",
        asset_type="stock",
        timestep="day",
        requested_start=datetime(2023, 7, 30, tzinfo=timezone.utc),
        requested_end=datetime(2026, 7, 30, tzinfo=timezone.utc),
        outcome=HistoryOutcome.PARTIAL,
        expected_sessions=753,
        returned_sessions=751,
        missing_sessions=["2025-04-29", "2025-04-30"],
        repair_attempts=1,
        reason="unresolved_daily_sessions_after_bounded_repair",
    )

    snapshot = ibkr_history_health_snapshot()

    assert snapshot["provider"] == "ibkr"
    assert snapshot["complete"] is False
    assert snapshot["incomplete_series_count"] == 1
    assert snapshot["series"][0]["missing_sessions"] == ["2025-04-29", "2025-04-30"]
    assert snapshot["series"][0]["missing_session_count"] == 2
    assert "api_key" not in str(snapshot).lower()


def test_health_snapshot_caps_missing_session_evidence() -> None:
    sessions = [
        timestamp.date().isoformat()
        for timestamp in pd.date_range("2025-01-01", periods=125, freq="D")
    ]
    record_history_health(
        symbol="SQQQ",
        asset_type="stock",
        timestep="day",
        requested_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        requested_end=datetime(2025, 12, 31, tzinfo=timezone.utc),
        outcome=HistoryOutcome.PARTIAL,
        missing_sessions=sessions,
    )

    health = ibkr_history_health_snapshot()["series"][0]
    assert len(health["missing_sessions"]) == 100
    assert health["missing_session_count"] == 125


def test_backtest_settings_include_sanitized_data_health(tmp_path) -> None:
    from lumibot.strategies.strategy import Strategy

    record_history_health(
        symbol="SQQQ",
        asset_type="stock",
        timestep="day",
        requested_start=datetime(2023, 7, 30, tzinfo=timezone.utc),
        requested_end=datetime(2026, 7, 30, tzinfo=timezone.utc),
        outcome=HistoryOutcome.PARTIAL,
        expected_sessions=753,
        returned_sessions=751,
        missing_sessions=["2025-04-29", "2025-04-30"],
    )
    fake = SimpleNamespace(
        broker=SimpleNamespace(data_source=SimpleNamespace(auto_adjust=False)),
        name="health-artifact-test",
        backtesting_start=datetime(2023, 7, 30, tzinfo=timezone.utc),
        backtesting_end=datetime(2026, 7, 30, tzinfo=timezone.utc),
        initial_budget=5000,
        risk_free_rate=0.0,
        minutes_before_closing=0,
        minutes_before_opening=0,
        sleeptime="1D",
        quote_asset=None,
        _benchmark_asset=None,
        starting_positions=None,
        parameters={},
    )
    output = tmp_path / "settings.json"

    Strategy.write_backtest_settings(fake, output.as_posix())

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["data_health"]["series"][0]["missing_sessions"] == [
        "2025-04-29",
        "2025-04-30",
    ]
    assert "credential" not in json.dumps(payload["data_health"]).lower()
