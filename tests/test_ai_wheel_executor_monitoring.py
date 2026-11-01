"""Actual executor timing; only the external AI response is substituted.

This is a native lifecycle test, not a model eval or a trading/performance result.
"""
from datetime import datetime

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.components.agents.manager import AgentHandle
from lumibot.entities import Asset, Data
from lumibot.example_strategies.ai_wheel import AIWheelStrategy
from lumibot.traders import Trader


def test_actual_executor_dispatches_management_in_final_fifteen_minutes(tmp_path, monkeypatch):
    monkeypatch.setenv("LUMIBOT_MEMORY_DIR", str(tmp_path / "memory"))
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.delenv("BACKTESTING_DATA_SOURCE", raising=False)
    monkeypatch.chdir(tmp_path)
    index = pd.date_range("2026-10-13 09:30", "2026-10-13 16:00", freq="min", tz="America/New_York")
    asset = Asset("XYZ")
    bars = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100., "volume": 1000}, index=index)
    source = PandasDataBacktesting(datetime_start=datetime(2026, 10, 13),
        datetime_end=datetime(2026, 10, 13, 16), pandas_data={asset: Data(asset, bars)}, show_progress_bar=False)
    broker = BacktestingBroker(source)
    strategy = AIWheelStrategy(broker=broker, name="native_monitoring", budget=100_000,
        parameters={"symbol": "XYZ"}, risk_free_rate=0, benchmark_asset=None, analyze_backtest=False,
        save_stats_file=False, stats_file=str(tmp_path / "stats.csv"))
    calls = []

    def response(handle, **kwargs):
        now = strategy.get_datetime()
        calls.append((handle.name, now))
        if handle.name == "trader" and len([c for c in calls if c[0] == "trader"]) == 1:
            strategy.agents.attention.watch(owner="trader", watch_id="late-management", operation_id="observation",
                metric="time", comparison="gte", value="2026-10-13T15:47:00-04:00",
                reason="Controlled management condition during the last fifteen minutes", review_scope="management")
            strategy.agents.attention.watch(owner="trader", watch_id="close-management", operation_id="observation",
                metric="time", comparison="gte", value="2026-10-13T16:00:00-04:00",
                reason="Controlled management condition at the closing observation", review_scope="management")
        return AgentRunResult(summary="Native timing fixture only", model="fixture", events=[],
            payload={"execution_outcome": {"decision_completed": True}})

    monkeypatch.setattr(AgentHandle, "run", response)
    trader = Trader(backtest=True, quiet_logs=True, logfile=str(tmp_path / "runtime.log"))
    trader.add_strategy(strategy)
    try:
        trader.run_all(show_plot=False, save_tearsheet=False, show_tearsheet=False, show_indicators=False)
        trader_calls = [at for role, at in calls if role == "trader"]
        assert any(at.hour == 9 and at.minute == 30 for at in trader_calls)
        assert any(at.hour == 15 and at.minute == 47 for at in trader_calls)
        assert any(at.hour == 16 and at.minute == 0 for at in trader_calls)
        assert [role for role, _ in calls] == ["researcher", "trader", "trader", "trader"]
    finally:
        broker.stream.stop()
