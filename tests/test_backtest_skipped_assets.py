"""One asset without history must not fail a multi-asset backtest.

A ranking strategy over many symbols asks for prices of names that are not listed yet on
early dates (or were delisted, or the provider has nothing for them). That asset has no
price on those dates: the strategy gets no bars for it and cannot trade it, a WARNING names
it once, and the run reports it under ``skipped_assets``. No price is invented. The run
still fails when no asset has any data at all, and a temporary download failure still
fails it, because then the data exists and the results would be wrong.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.tools.ibkr_history_health import (
    HistoryOutcome,
    ibkr_history_health_snapshot,
    record_history_health,
    record_required_history,
    reset_ibkr_history_health_for_testing,
)

from lumibot.entities import Asset

LATE = Asset("LATECO", asset_type=Asset.AssetType.STOCK)
BIG = Asset("BIGCO", asset_type=Asset.AssetType.STOCK)


@pytest.fixture(autouse=True)
def _reset():
    reset_ibkr_history_health_for_testing()
    yield
    reset_ibkr_history_health_for_testing()


def _bars(day, count=1):
    index = pd.bdate_range(end=pd.Timestamp(day), periods=count)
    return pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=index)


def _when(day):
    return datetime.fromisoformat(f"{day}T14:30:00").replace(tzinfo=timezone.utc)


def test_asset_with_no_bars_on_some_dates_is_skipped_not_failed(caplog):
    caplog.set_level(logging.WARNING)
    for day in ("2020-01-02", "2020-01-03", "2020-01-06"):
        record_required_history(asset=BIG, timestep="day", requested_bars=1, frame=_bars(day), when=_when(day))
        record_required_history(asset=LATE, timestep="day", requested_bars=1, frame=None, when=_when(day))
    record_required_history(asset=LATE, timestep="day", requested_bars=1, frame=_bars("2020-10-01"),
                            when=_when("2020-10-01"))

    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is True
    assert health["required_failures"] == []
    skipped = health["skipped_assets"]
    assert [entry["symbol"] for entry in skipped] == ["LATECO"]
    assert skipped[0]["first_date"] == "2020-01-02"
    assert skipped[0]["last_date"] == "2020-01-06"
    assert skipped[0]["occurrences"] == 3
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "LATECO" in r.getMessage()]
    assert len(warnings) == 1


def test_only_asset_with_no_data_at_all_still_fails():
    for day in ("2020-01-02", "2020-01-03"):
        record_required_history(asset=LATE, timestep="day", requested_bars=1, frame=None, when=_when(day))
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is False
    assert health["required_failures"][0]["symbol"] == "LATECO"


def test_temporary_download_failure_still_fails_the_run():
    record_required_history(asset=BIG, timestep="day", requested_bars=1, frame=_bars("2020-01-02"),
                            when=_when("2020-01-02"))
    record_history_health(symbol="LATECO", asset_type="stock", timestep="day",
                          requested_start=datetime(2019, 12, 1, tzinfo=timezone.utc),
                          requested_end=datetime(2020, 2, 1, tzinfo=timezone.utc),
                          outcome=HistoryOutcome.TRANSIENT_FAILURE, reason="rate_limited")
    record_required_history(asset=LATE, timestep="day", requested_bars=1, frame=None, when=_when("2020-01-02"))
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is False
    assert health.get("skipped_assets", []) == []


def _strategy_with(seen):
    from lumibot.strategies.strategy import Strategy

    holder = SimpleNamespace(_backtest_data_seen=seen, get_datetime=lambda: _when("2020-01-02"),
                             broker=SimpleNamespace(data_source=SimpleNamespace(SOURCE="test")))
    holder.__dict__["_backtest_data_seen"] = seen
    return Strategy._report_assets_without_data, holder


def test_asset_that_never_returned_data_is_skipped_when_others_did():
    report, holder = _strategy_with({
        BIG: {"found": True, "requests": 5, "first": _when("2020-01-02")},
        LATE: {"found": False, "requests": 5, "first": _when("2020-01-02")},
    })
    report(holder)
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is True
    assert [entry["symbol"] for entry in health["skipped_assets"]] == ["LATECO"]


def test_no_asset_returned_data_still_fails():
    report, holder = _strategy_with({LATE: {"found": False, "requests": 5, "first": _when("2020-01-02")}})
    report(holder)
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is False
    assert health["required_failures"][0]["symbol"] == "LATECO"
