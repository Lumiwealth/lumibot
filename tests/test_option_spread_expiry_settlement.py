"""A spread held to expiry settles to its net value, never to stock the account cannot hold.

When both legs of a vertical spread finish in the money, exercising the long leg and being
assigned on the short leg deliver offsetting shares. The account ends with the spread's
width in cash (for a debit spread) and no stock, whatever its size: a small account that
could never buy 100 shares must not be left long or short 100 shares.
"""
from datetime import date, datetime

import pandas as pd
import pandas_market_calendars as calendars
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy

EXPIRY = date(2026, 10, 13)
UNDERLYING = Asset("XYZ")


def _option(strike, right):
    return Asset("XYZ", asset_type="option", expiration=EXPIRY, strike=strike, right=right)


def _run(tmp_path, legs, option_prices, stock_at_expiry, budget):
    schedule = calendars.get_calendar("NYSE").schedule(start_date="2026-10-12", end_date="2026-10-14")
    index = pd.DatetimeIndex([])
    for session in schedule.itertuples():
        bars = pd.date_range(session.market_open, session.market_close, freq="min").tz_convert("America/New_York")
        index = bars if index.empty else index.append(bars)
    stock = [105.0 if day < EXPIRY else stock_at_expiry for day in index.date]
    data = {}
    for asset in [UNDERLYING] + [asset for asset, _ in legs]:
        prices = stock if asset == UNDERLYING else option_prices[asset]
        frame = pd.DataFrame(
            {"open": prices, "high": prices, "low": prices, "close": prices, "volume": 1000}, index=index
        )
        data[asset] = Data(asset, frame, timestep="minute")

    class RecordedSource(PandasDataBacktesting):
        SOURCE = "RECORDED"

    class SpreadToExpiry(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.minutes_before_closing = 0
            self.vars.opened = False
            self.vars.max_stock = 0.0

        def on_trading_iteration(self):
            if not self.vars.opened:
                orders = [self.create_order(asset, 1, side) for asset, side in legs]
                self.submit_order(orders, is_multileg=True)
                self.vars.opened = True
            stock = self.get_position(UNDERLYING)
            if stock is not None:
                self.vars.max_stock = max(self.vars.max_stock, abs(float(stock.quantity)))

    _, strategy = SpreadToExpiry.run_backtest(
        RecordedSource,
        backtesting_start=datetime(2026, 10, 12), backtesting_end=datetime(2026, 10, 14, 16),
        pandas_data=data, budget=budget, benchmark_asset=None, risk_free_rate=0,
        analyze_backtest=False, show_progress_bar=False, show_plot=False,
        save_tearsheet=False, show_tearsheet=False, stats_file=str(tmp_path / "stats.csv"),
    )
    return strategy


def test_call_debit_spread_in_the_money_at_expiry_pays_the_width_in_cash(tmp_path):
    long_call, short_call = _option(100, "call"), _option(110, "call")
    strategy = _run(
        tmp_path,
        [(long_call, "buy"), (short_call, "sell")],
        {long_call: 7.0, short_call: 2.0},
        stock_at_expiry=120.0,
        budget=5_000,  # far less than 100 shares of XYZ
    )
    assert strategy.get_position(UNDERLYING) is None
    assert strategy.vars.max_stock == 0.0
    assert strategy.get_position(long_call) is None
    assert strategy.get_position(short_call) is None
    # 5,000 - 500 debit + 1,000 width
    assert float(strategy.get_cash()) == pytest.approx(5_500.0)


def test_put_credit_spread_in_the_money_at_expiry_costs_the_width_in_cash(tmp_path):
    short_put, long_put = _option(110, "put"), _option(100, "put")
    strategy = _run(
        tmp_path,
        [(short_put, "sell"), (long_put, "buy")],
        {short_put: 8.0, long_put: 3.0},
        stock_at_expiry=90.0,
        budget=5_000,
    )
    assert strategy.get_position(UNDERLYING) is None
    assert strategy.vars.max_stock == 0.0
    # 5,000 + 500 credit - 1,000 width
    assert float(strategy.get_cash()) == pytest.approx(4_500.0)
