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


def test_required_short_history_stays_invalid_after_later_success():
    from lumibot.tools.ibkr_history_health import record_required_history

    asset = SimpleNamespace(symbol="MGC", asset_type="cont_future")
    when = pd.Timestamp("2026-10-01 12:00", tz="UTC")
    frame = pd.DataFrame({"close": range(32)}, index=pd.date_range(end=when, periods=32, freq="B"))
    record_required_history(asset=asset, timestep="day", requested_bars=51, frame=frame, when=when)
    complete = pd.DataFrame({"close": range(51)}, index=pd.date_range(end=when, periods=51, freq="B"))
    record_required_history(asset=asset, timestep="day", requested_bars=51, frame=complete,
                            when=when + pd.Timedelta(days=1))
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is False
    assert health["required_failures"][0]["reason"] == "insufficient_required_history"
    assert health["required_failures"][0]["requested_bars"] == 51
    assert health["required_failures"][0]["returned_bars"] == 32


def test_prefetch_gap_does_not_invalidate_satisfied_strategy_requirement():
    from lumibot.tools.ibkr_history_health import record_required_history

    when = pd.Timestamp("2026-10-01 12:00", tz="UTC")
    record_history_health(symbol="SPY", asset_type="stock", timestep="day",
                          requested_start=when - pd.Timedelta(days=1000), requested_end=when,
                          outcome=HistoryOutcome.PARTIAL, missing_sessions=["2024-01-02"])
    frame = pd.DataFrame({"close": range(5)}, index=pd.date_range(end=when, periods=5, freq="B"))
    record_required_history(asset=SimpleNamespace(symbol="SPY", asset_type="stock"), timestep="day",
                            requested_bars=5, frame=frame, when=when)
    health = ibkr_history_health_snapshot()
    assert health["complete"] is False
    assert health["required_complete"] is True


def test_required_history_detects_known_gap_inside_consumed_interval():
    from lumibot.tools.ibkr_history_health import record_required_history

    when = pd.Timestamp("2026-10-01 12:00", tz="UTC")
    record_history_health(symbol="SPY", asset_type="stock", timestep="day",
                          requested_start=when - pd.Timedelta(days=10), requested_end=when,
                          outcome=HistoryOutcome.PARTIAL, missing_sessions=["2026-09-29"])
    frame = pd.DataFrame({"close": range(5)}, index=pd.date_range(end=when, periods=5, freq="B"))
    record_required_history(asset=SimpleNamespace(symbol="SPY", asset_type="stock"), timestep="day",
                            requested_bars=5, frame=frame, when=when)
    assert ibkr_history_health_snapshot()["required_failures"][0]["reason"] == "missing_required_sessions"


@pytest.mark.parametrize("gap_start,expected", [("2026-09-29", False), ("2024-01-02", True)])
def test_required_continuous_history_detects_unresolved_contract_with_enough_rows(gap_start, expected):
    from lumibot.tools.ibkr_history_health import record_required_history

    when = pd.Timestamp("2026-10-01 20:00", tz="UTC")
    start = pd.Timestamp(gap_start, tz="UTC")
    record_history_health(symbol="MGC", asset_type="cont_future", timestep="day",
                          requested_start=start, requested_end=start + pd.Timedelta(days=1),
                          outcome=HistoryOutcome.PARTIAL, reason="unresolved_roll_contract")
    # Enough older bars can conceal a missing contract segment in the middle.
    frame = pd.DataFrame({"close": range(5)}, index=pd.date_range(end=when, periods=5, freq="B"))
    record_required_history(asset=SimpleNamespace(symbol="MGC", asset_type="cont_future"),
                            timestep="day", requested_bars=5, frame=frame, when=when)
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is expected
    if not expected:
        assert health["required_failures"][0]["reason"] == "unresolved_required_contract"


@pytest.mark.parametrize("provider,expected", [("ibkr", False), ("thetadata", True)])
def test_routed_requirement_telemetry_applies_only_to_ibkr(provider, expected):
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    from lumibot.entities import Asset

    router = SimpleNamespace(
        _provider_spec_for_asset=lambda asset: SimpleNamespace(provider=provider),
        get_datetime=lambda: pd.Timestamp("2026-10-01", tz="UTC"),
    )
    RoutedBacktestingPandas.record_history_requirement(
        router, asset=Asset("MGC", asset_type="cont_future"), timestep="day", requested_bars=51, bars=None)
    assert ibkr_history_health_snapshot()["required_complete"] is expected


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


def test_health_does_not_overwrite_another_required_window():
    common = dict(symbol="TQQQ", asset_type="stock", timestep="day")
    record_history_health(**common, requested_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        requested_end=datetime(2024, 2, 1, tzinfo=timezone.utc), outcome=HistoryOutcome.PARTIAL,
        reason="unresolved_daily_sessions_after_bounded_repair")
    record_history_health(**common, requested_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        requested_end=datetime(2025, 2, 1, tzinfo=timezone.utc), outcome=HistoryOutcome.COMPLETE)
    snapshot = ibkr_history_health_snapshot()
    assert snapshot["series_count"] == 2
    assert snapshot["incomplete_series_count"] == 1


def test_health_separates_sources_deduplicates_events_and_accepts_repair():
    common = dict(symbol="TQQQ", asset_type="stock", timestep="day",
                  requested_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                  requested_end=datetime(2025, 2, 1, tzinfo=timezone.utc))
    for _ in range(2):
        record_history_health(**common, series_id="trades-rth", event_id="failed-fetch",
                              outcome=HistoryOutcome.TRANSIENT_FAILURE, transient_failures=1, reason="rate_limited")
    record_history_health(**common, series_id="midpoint-rth", outcome=HistoryOutcome.COMPLETE)
    snapshot = ibkr_history_health_snapshot()
    assert snapshot["series_count"] == 2 and snapshot["incomplete_series_count"] == 1
    failed = next(row for row in snapshot["series"] if row["series_id"] == "trades-rth")
    assert failed["transient_failures"] == 1
    record_history_health(**common, series_id="trades-rth", outcome=HistoryOutcome.COMPLETE,
                          expected_sessions=21, returned_sessions=21)
    snapshot = ibkr_history_health_snapshot()
    assert snapshot["complete"] and snapshot["incomplete_series_count"] == 0


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
