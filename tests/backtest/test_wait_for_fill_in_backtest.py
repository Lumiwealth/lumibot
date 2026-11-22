"""Waiting for a fill inside one backtest iteration must not hang the run.

In a backtest a market order fills when the broker processes pending orders after the
iteration, so an iteration that waits for its own fill waited forever (a hosted run sat
39 minutes). Waiting cannot move simulated time: the call returns the order as it is.
"""
import threading
from datetime import datetime

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy
from lumibot.traders import Trader


def _run(tmp_path, wait):
    index = pd.date_range("2026-10-26 09:30", "2026-10-26 11:00", freq="min", tz="America/New_York")
    frame = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100., "volume": 1000}, index=index)
    asset = Asset("XYZ")
    source = PandasDataBacktesting(
        datetime_start=datetime(2026, 10, 26, 9, 30), datetime_end=datetime(2026, 10, 26, 10, 30),
        pandas_data={asset: Data(asset, frame, timestep="minute")}, show_progress_bar=False,
    )

    class WaitsForItsFill(Strategy):
        def initialize(self):
            self.sleeptime = "10M"
            self.vars.after_wait = []

        def on_trading_iteration(self):
            if self.vars.after_wait:
                return
            order = self.submit_order(self.create_order(asset, 10, "buy"))
            wait(self, order)
            self.vars.after_wait.append(order.status)

    broker = BacktestingBroker(source)
    strategy = WaitsForItsFill(broker=broker, budget=10000, benchmark_asset=None, risk_free_rate=0,
                               analyze_backtest=False, stats_file=str(tmp_path / "stats.csv"))
    trader = Trader(backtest=True)
    trader.add_strategy(strategy)
    done = threading.Event()

    def go():
        trader.run_all(trades_file=str(tmp_path / "trades.csv"), show_plot=False,
                       save_tearsheet=False, show_tearsheet=False, show_indicators=False)
        done.set()

    threading.Thread(target=go, daemon=True).start()
    assert done.wait(60), "the backtest hung waiting for a fill that only happens after the iteration"
    return strategy


def test_wait_for_order_execution_returns_in_a_backtest(tmp_path):
    strategy = _run(tmp_path, lambda s, o: s.wait_for_order_execution(o))
    assert len(strategy.vars.after_wait) == 1
    assert strategy.get_position(Asset("XYZ")).quantity == 10  # filled after the iteration, as before


def test_wait_for_orders_execution_returns_in_a_backtest(tmp_path):
    strategy = _run(tmp_path, lambda s, o: s.wait_for_orders_execution([o]))
    assert strategy.get_position(Asset("XYZ")).quantity == 10
