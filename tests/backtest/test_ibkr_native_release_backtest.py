"""Real IBKR adapter/engine proof with failed optional metadata and fixture HTTP bars.

Only external transports/contract lookup are replaced. The normal price helper,
cache, adapter, strategy, order matching and accounting must all execute.
"""

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.tools import ibkr_helper as h
from lumibot.tools.ibkr_history_health import ibkr_history_health_snapshot, reset_ibkr_history_health
from lumibot.tools.yahoo_helper import YahooHelper


class _NativeStockSmoke(Strategy):
    def initialize(self):
        self.sleeptime = "5M"
        self.asset = Asset("SPY", asset_type="stock")
        self.submitted = False

    def on_trading_iteration(self):
        bars = self.get_historical_prices(self.asset, 5, timestep="minute", include_after_hours=False)
        if bars is not None and len(bars.df) >= 5 and not self.submitted:
            self.submit_order(self.create_order(self.asset, 10, "buy"))
            self.submitted = True


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
def test_ibkr_engine_completes_and_fills_without_corporate_action_service(monkeypatch, tmp_path):
    reset_ibkr_history_health()
    monkeypatch.setattr(h, "LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.setattr(h, "_IBKR_EQUITY_ACTIONS_CACHE", {})
    monkeypatch.setattr(h, "_IBKR_EQUITY_BASIS_CACHE", {})
    monkeypatch.setattr(h, "_ibkr_history_now_utc", lambda: pd.Timestamp("2026-10-22T00:42Z").to_pydatetime())
    monkeypatch.setattr(h, "get_backtest_cache", lambda: SimpleNamespace(
        ensure_local_file=lambda *a, **k: None, on_local_update=lambda *a, **k: None))
    monkeypatch.setattr(h, "_resolve_conid", lambda **_: 12345)
    metadata_calls, price_calls = [], []

    def unavailable(*a, **k):
        metadata_calls.append(1)
        raise TimeoutError("optional metadata service unavailable")

    monkeypatch.setattr(YahooHelper, "get_symbol_data", unavailable)
    sessions = pd.bdate_range("2024-06-03", "2024-06-14", tz="America/New_York")
    minutes = pd.DatetimeIndex([stamp for day in sessions for stamp in pd.date_range(
        day + pd.Timedelta(hours=9, minutes=30), periods=390, freq="min")])

    def history(**request):
        assert request["source"] == "Trades"
        price_calls.append(request["bar"])
        end = pd.to_datetime(request["start_time"], format="%Y%m%d-%H:%M:%S", utc=True)
        index = sessions + pd.Timedelta(hours=16) if request["bar"] == "1d" else minutes
        index = index[index.tz_convert("UTC") <= end][-1000:]
        return {"volumeFactor": 1, "data": [
            {"t": int(stamp.timestamp() * 1000), "o": 100., "h": 103., "l": 99., "c": 102., "v": 10000.}
            for stamp in index]}

    monkeypatch.setattr(h, "_ibkr_history_request", history)
    try:
        _, strategy = _NativeStockSmoke.run_backtest(
            InteractiveBrokersRESTBacktesting, datetime(2024, 6, 11), datetime(2024, 6, 13),
            budget=10000, benchmark_asset=None, analyze_backtest=False,
            show_plot=False, show_tearsheet=False, save_tearsheet=False,
            show_indicators=False, save_logfile=False, show_progress_bar=False,
        )
        assert metadata_calls and price_calls
        fills = strategy.broker._trade_event_log_df
        fills = fills[fills["status"] == "fill"]
        assert list(fills["symbol"]) == ["SPY"]
        assert list(fills["price"].astype(float)) == [100.]
        assert float(strategy.get_position("SPY").quantity) == 10
        assert strategy.cash == pytest.approx(9000.)
        assert strategy.get_datetime().date() >= datetime(2024, 6, 12).date()
        health = ibkr_history_health_snapshot()
        assert health["warnings"][0]["cash_dividends_verified"] is False
        assert health["required_complete"] is True
    finally:
        reset_ibkr_history_health()
