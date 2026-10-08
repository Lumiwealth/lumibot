"""Portable Alpaca acceptance: SDK response fixtures, real adapter and engine.

Only the external SDK transport is replaced. No vendor subscription, network
cache, model, or live broker is involved; orders and accounting are real.
"""

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.backtesting import AlpacaBacktesting
from lumibot.strategies import Strategy


class _BuyOnce(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.submitted = False

    def on_trading_iteration(self):
        if not self.submitted:
            self.submit_order(self.create_order("SPY", 10, "buy"))
            self.submitted = True


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
def test_alpaca_sdk_bars_produce_real_fills_and_correct_cash(monkeypatch, tmp_path):
    from lumibot.backtesting import alpaca_backtesting

    monkeypatch.setattr(alpaca_backtesting, "LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    requests = []

    class FixtureClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_stock_bars(self, request):
            requests.append(request)
            assert request.symbol_or_symbols == "SPY"
            frame = pd.DataFrame(
                {"open": 100.0, "high": 103.0, "low": 99.0, "close": 102.0, "volume": 10000},
                index=pd.bdate_range("2026-02-09", periods=7, tz="America/New_York"),
            )
            frame.index.name = "timestamp"
            frame["symbol"] = "SPY"
            return SimpleNamespace(df=frame.reset_index().set_index(["symbol", "timestamp"]))

    monkeypatch.setattr(alpaca_backtesting, "StockHistoricalDataClient", FixtureClient)
    _, strategy = _BuyOnce.run_backtest(
        AlpacaBacktesting, datetime(2026, 2, 9), datetime(2026, 2, 13),
        config={"API_KEY": "fixture-key", "API_SECRET": "fixture-secret", "PAPER": True},
        budget=10000, benchmark_asset=None, analyze_backtest=False,
        show_plot=False, show_tearsheet=False, save_tearsheet=False,
        show_indicators=False, save_logfile=False, show_progress_bar=False,
    )
    assert requests, "The real Alpaca adapter must request its bars through the SDK"
    fills = strategy.broker._trade_event_log_df
    fills = fills[fills["status"] == "fill"]
    assert list(fills["symbol"]) == ["SPY"]
    assert list(fills["price"].astype(float)) == [100.0]  # open, never the future close
    assert float(strategy.get_position("SPY").quantity) == 10
    assert strategy.cash == pytest.approx(9000.0)
