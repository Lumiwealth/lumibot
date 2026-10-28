"""Research tool failures are recoverable; consumed strategy inputs stay strict."""

import json
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.components.agents.builtins import (
    _bind_get_indicator, _bind_get_indicators, _bind_historical_prices, _bind_load_history,
)
from lumibot.entities import Asset
from lumibot.indicators import Indicators
from lumibot.tools.ibkr_history_health import (
    HistoryOutcome, ibkr_history_health_snapshot, record_history_health,
    record_required_history, reset_ibkr_history_health_for_testing,
)


@pytest.fixture(autouse=True)
def health_state():
    reset_ibkr_history_health_for_testing()
    yield
    reset_ibkr_history_health_for_testing()


def research_strategy():
    index = pd.date_range("2025-01-02", periods=12, freq="B", tz="UTC")
    now = index[-1] + pd.Timedelta(hours=16)
    bad, good = Asset("ALPHA"), Asset("BETA")
    complete = pd.DataFrame({"close": range(100, 112)}, index=index)
    partial = complete.drop(index[-2])
    record_history_health(symbol=bad.symbol, asset_type="stock", timestep="day",
        requested_start=index[0], requested_end=now, outcome=HistoryOutcome.PARTIAL,
        missing_sessions=[index[-2].date().isoformat()])

    def record_input(*, asset, timestep, frame):
        record_required_history(asset=asset, timestep=timestep, requested_bars=len(frame),
                                frame=frame, when=now)

    source = SimpleNamespace(
        _data_store={(bad, "day"): SimpleNamespace(df=partial, timestep="day"),
                     (good, "day"): SimpleNamespace(df=complete, timestep="day")},
        record_indicator_input=record_input,
    )
    strategy = SimpleNamespace(broker=SimpleNamespace(data_source=source),
        get_datetime=lambda: now, is_backtesting=True,
        quote_asset=Asset("USD", "forex"))
    strategy.indicators = Indicators(strategy)
    return strategy, bad, good


def test_failed_candidate_indicator_is_visible_without_poisoning_successful_research():
    strategy, bad, good = research_strategy()
    result = _bind_get_indicators(strategy, None).function(bad.symbol, requests_json=json.dumps([
        {"id": "candidate", "symbol": bad.symbol, "indicator": "sma", "parameters": {"length": 3}},
        {"id": "alternative", "symbol": good.symbol, "indicator": "sma", "parameters": {"length": 3}},
    ]))
    assert result["complete"] is False
    failed, valid = result["results"]
    assert failed["ok"] is False
    assert "missing_required_sessions" in failed["error"]
    assert "value" not in failed
    assert valid["ok"] is True
    assert valid["value"] == pytest.approx(110)
    health = ibkr_history_health_snapshot()
    assert health["complete"] is False, "Keep the original provider gap evidence"
    assert health["required_complete"] is True
    assert health["warnings"][0]["reason"] == "missing_required_sessions"


def test_direct_strategy_indicator_remains_required():
    strategy, bad, _ = research_strategy()
    strategy.indicators.calculate(bad, "sma", parameters={"length": 3})
    assert ibkr_history_health_snapshot()["required_complete"] is False


def test_failed_tool_does_not_clear_an_existing_required_failure():
    strategy, bad, good = research_strategy()
    record_required_history(asset=good, timestep="valuation", requested_bars=1,
        frame=None, when=strategy.get_datetime(), requirement="valuation")
    tool = _bind_get_indicators(strategy, None).function
    result = tool(bad.symbol, indicators=["sma"])
    assert result["complete"] is False
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is False
    assert [f["reason"] for f in health["required_failures"]] == ["missing_valuation_price"]


def test_single_indicator_failure_restores_scope_before_later_strategy_read():
    strategy, bad, _ = research_strategy()
    with pytest.raises(ValueError, match="missing_required_sessions"):
        _bind_get_indicator(strategy, None).function(bad.symbol, "sma")
    assert ibkr_history_health_snapshot()["required_complete"] is True
    strategy.indicators.calculate(bad, "sma", parameters={"length": 3})
    assert ibkr_history_health_snapshot()["required_complete"] is False


def test_history_fallback_reports_failed_candidate_and_keeps_valid_alternative():
    strategy, bad, good = research_strategy()
    source = strategy.broker.data_source

    def history(asset, length, **kwargs):
        frame = source._data_store[(asset, "day")].df.tail(length)
        record_required_history(asset=asset, timestep="day", requested_bars=length,
            frame=frame, when=strategy.get_datetime())
        return SimpleNamespace(pandas_df=frame, df=frame, timestep="day")

    strategy.get_historical_prices = history
    result = _bind_historical_prices(strategy, None).function(
        symbols=[bad.symbol, good.symbol], length=3, timestep="day")
    assert result["symbols_missing"] == [bad.symbol]
    assert result["symbols_available"] == [good.symbol]
    assert result["bars_by_symbol"][bad.symbol] == []
    assert "missing_required_sessions" in result["symbols_data_errors"][bad.symbol]
    assert ibkr_history_health_snapshot()["required_complete"] is True


def test_single_history_table_cannot_register_incomplete_input():
    strategy, bad, _ = research_strategy()
    source = strategy.broker.data_source

    def load(**kwargs):
        frame = source._data_store[(bad, "day")].df
        record_required_history(asset=bad, timestep="day", requested_bars=len(frame),
            frame=frame, when=strategy.get_datetime())
        pytest.fail("Incomplete history reached table registration")

    manager = SimpleNamespace(duckdb=SimpleNamespace(load_history_table=load))
    with pytest.raises(ValueError, match="missing_required_sessions"):
        _bind_load_history(strategy, manager).function(symbol=bad.symbol, length=12)
    assert ibkr_history_health_snapshot()["required_complete"] is True


def test_research_scope_cannot_downgrade_valuation_and_does_not_leak_to_other_threads():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from lumibot.tools.ibkr_history_health import recoverable_history_request

    when = pd.Timestamp("2025-01-20T16:00:00Z")
    barrier = Barrier(2)

    def research():
        with recoverable_history_request():
            record_required_history(asset=Asset("VALUED"), timestep="valuation", requested_bars=1,
                frame=None, when=when, requirement="valuation")
            barrier.wait()
            barrier.wait()

    def decision():
        barrier.wait()
        try:
            record_required_history(asset=Asset("REQUIRED"), timestep="day", requested_bars=1,
                frame=None, when=when)
        finally:
            barrier.wait()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(research), pool.submit(decision)]
        for future in futures:
            future.result()
    health = ibkr_history_health_snapshot()
    assert {(r["symbol"], r["reason"]) for r in health["required_failures"]} == {
        ("VALUED", "missing_valuation_price"), ("REQUIRED", "insufficient_required_history")}
    assert health["warnings"] == []
