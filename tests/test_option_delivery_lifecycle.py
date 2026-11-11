"""Controlled physical settlement mechanics, not an AI or historical return test."""
from datetime import date, datetime

import pandas as pd
import pandas_market_calendars as calendars
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


def test_physical_delivery_round_trip_then_worthless_expiry_and_reentry(tmp_path):
    underlying = Asset("XYZ")
    put = Asset("XYZ", asset_type="option", expiration=date(2026, 10, 13), strike=100, right="put")
    call = Asset("XYZ", asset_type="option", expiration=date(2026, 10, 15), strike=100, right="call")
    worthless = Asset("XYZ", asset_type="option", expiration=date(2026, 10, 19), strike=100, right="put")
    next_put = Asset("XYZ", asset_type="option", expiration=date(2026, 10, 30), strike=100, right="put")
    schedule = calendars.get_calendar("NYSE").schedule(start_date="2026-10-12", end_date="2026-10-21")
    index = pd.DatetimeIndex([])
    for session in schedule.itertuples():
        bars = pd.date_range(session.market_open, session.market_close, freq="min").tz_convert("America/New_York")
        index = bars if index.empty else index.append(bars)
    stock = [95. if day <= date(2026, 10, 13) else 105. for day in index.date]
    data = {}
    for asset in [underlying, put, call, worthless, next_put]:
        prices = stock if asset == underlying else (2. if asset == put else 1.)
        frame = pd.DataFrame({"open": prices, "high": prices, "low": prices, "close": prices, "bid": prices, "ask": prices, "volume": 1000}, index=index)
        data[asset] = Data(asset, frame, timestep="minute")

    class RecordedSource(PandasDataBacktesting):
        # Exercise settlement without the historical PANDAS-only source guard.
        SOURCE = "RECORDED"

    class DeliverySequence(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.minutes_before_closing = 0
            self.vars.stage = 0
            self.vars.events = []
            self.vars.seen_worthless = False

        def on_trading_iteration(self):
            shares = self.get_position(underlying)
            share_qty = float(shares.quantity) if shares else 0.
            if self.vars.stage == 0:
                self.submit_order(self.create_order(put, 1, "sell_to_open", limit_price=2.))
                self.vars.events.append("short_put")
                self.vars.stage = 1
            elif self.vars.stage == 1 and share_qty == 100 and not self.get_position(put):
                self.vars.events.append("assigned_shares")
                self.submit_order(self.create_order(call, 1, "sell_to_open", limit_price=1.))
                self.vars.stage = 2
            elif self.vars.stage == 2 and share_qty == 0 and not self.get_position(call):
                self.vars.events.append("called_away_cash")
                self.submit_order(self.create_order(worthless, 1, "sell_to_open", limit_price=1.))
                self.vars.stage = 3
            elif self.vars.stage == 3:
                if self.get_position(worthless):
                    self.vars.seen_worthless = True
                elif self.vars.seen_worthless:
                    self.vars.events.append("worthless_expiry")
                    self.submit_order(self.create_order(next_put, 1, "sell_to_open", limit_price=1.))
                    self.vars.stage = 4
            elif self.vars.stage == 4 and self.get_position(next_put):
                if "reentry" not in self.vars.events:
                    self.vars.events.append("reentry")

    _, strategy = DeliverySequence.run_backtest(RecordedSource,
        backtesting_start=datetime(2026, 10, 12), backtesting_end=datetime(2026, 10, 21, 16),
        pandas_data=data, budget=100_000, benchmark_asset=None, risk_free_rate=0,
        analyze_backtest=False, show_progress_bar=False, show_plot=False,
        save_tearsheet=False, show_tearsheet=False, stats_file=str(tmp_path / "stats.csv"))
    assert strategy.vars.events == ["short_put", "assigned_shares", "called_away_cash", "worthless_expiry", "reentry"]
    assert strategy.get_position(underlying) is None
    assert float(strategy.get_position(next_put).quantity) == -1.
    assert float(strategy.get_cash()) == pytest.approx(100_500.)
    assert float(strategy.portfolio_value) == pytest.approx(100_400.)
