"""Option lifecycles a backtest must get right end to end: the wheel and cash-settled index options.

A wheel sells a cash-secured put; when the put finishes in the money the account is assigned
100 shares per contract, then sells covered calls on those shares; when a call finishes in the
money the shares are called away and the account is back to cash. Index options (SPX, NDX, RUT,
XSP) never deliver shares: an index iron condor held to expiry settles to its value in cash and
leaves no position at all. These run real backtests on recorded bars, so they cover the broker,
the strategy positions and the cash together.
"""
from datetime import date, datetime

import pandas as pd
import pandas_market_calendars as calendars
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


class _RecordedSource(PandasDataBacktesting):
    SOURCE = "RECORDED"


def _minutes(start, end):
    schedule = calendars.get_calendar("NYSE").schedule(start_date=start, end_date=end)
    index = pd.DatetimeIndex([])
    for session in schedule.itertuples():
        bars = pd.date_range(session.market_open, session.market_close, freq="min").tz_convert("America/New_York")
        index = bars if index.empty else index.append(bars)
    return index


def _data(asset, index, price_for_day):
    prices = [price_for_day(day) for day in index.date]
    frame = pd.DataFrame({"open": prices, "high": prices, "low": prices, "close": prices, "volume": 1000}, index=index)
    return Data(asset, frame, timestep="minute")


def _run(strategy_class, data, start, end, budget, tmp_path):
    _, strategy = strategy_class.run_backtest(
        _RecordedSource,
        backtesting_start=start, backtesting_end=end,
        pandas_data=data, budget=budget, benchmark_asset=None, risk_free_rate=0,
        analyze_backtest=False, show_progress_bar=False, show_plot=False,
        save_tearsheet=False, show_tearsheet=False, stats_file=str(tmp_path / "stats.csv"),
    )
    return strategy


def test_wheel_put_assigned_then_shares_called_away(tmp_path):
    stock = Asset("XYZ")
    first_expiry, second_expiry = date(2026, 10, 16), date(2026, 10, 23)
    put = Asset("XYZ", asset_type="option", expiration=first_expiry, strike=100, right="put")
    call = Asset("XYZ", asset_type="option", expiration=second_expiry, strike=105, right="call")
    index = _minutes("2026-10-14", "2026-10-26")

    def stock_price(day):
        if day < first_expiry:
            return 102.0
        if day == first_expiry:
            return 95.0  # put finishes $5 in the money: assigned
        if day < second_expiry:
            return 100.0
        return 110.0  # call finishes $5 in the money: called away

    data = {
        stock: _data(stock, index, stock_price),
        put: _data(put, index, lambda day: 2.0),
        call: _data(call, index, lambda day: 1.5),
    }

    class Wheel(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.vars.log = []

        def on_trading_iteration(self):
            today = self.get_datetime().date()
            shares = self.get_position(stock)
            held = float(shares.quantity) if shares is not None else 0.0
            self.vars.log.append((today, held))
            if today == date(2026, 10, 14):
                self.submit_order(self.create_order(put, 1, "sell"))
            elif today == date(2026, 10, 19) and held >= 100:
                self.submit_order(self.create_order(call, 1, "sell"))

    strategy = _run(Wheel, data, datetime(2026, 10, 14), datetime(2026, 10, 26, 16), 20_000, tmp_path)
    held_by_day = dict(strategy.vars.log)
    assert held_by_day[date(2026, 10, 19)] == pytest.approx(100.0)  # assigned on the put
    assert held_by_day[date(2026, 10, 26)] == pytest.approx(0.0)  # shares called away
    assert strategy.get_position(put) is None
    assert strategy.get_position(call) is None
    # 20,000 + 200 put premium - 10,000 assignment + 150 call premium + 10,500 called away
    assert float(strategy.get_cash()) == pytest.approx(20_850.0)


@pytest.mark.parametrize("symbol", ["SPX", "NDX", "RUT", "XSP"])
def test_index_iron_condor_held_to_expiry_settles_in_cash_with_no_position(symbol, tmp_path):
    index_asset = Asset(symbol, asset_type="index")
    expiry = date(2026, 10, 16)

    def leg(strike, right):
        return Asset(symbol, asset_type="option", expiration=expiry, strike=strike, right=right,
                     underlying_asset=index_asset)

    long_put, short_put, short_call, long_call = leg(95, "put"), leg(100, "put"), leg(110, "call"), leg(115, "call")
    legs = [(long_put, "buy", 0.5), (short_put, "sell", 1.5), (short_call, "sell", 1.5), (long_call, "buy", 0.5)]
    minutes = _minutes("2026-10-15", "2026-10-19")
    data = {index_asset: _data(index_asset, minutes, lambda day: 92.0 if day >= expiry else 105.0)}
    for asset, _, price in legs:
        data[asset] = _data(asset, minutes, lambda day, price=price: price)

    class Condor(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.vars.opened = False

        def on_trading_iteration(self):
            if not self.vars.opened:
                self.submit_order([self.create_order(asset, 1, side) for asset, side, _ in legs], is_multileg=True)
                self.vars.opened = True

    strategy = _run(Condor, data, datetime(2026, 10, 15), datetime(2026, 10, 19, 16), 5_000, tmp_path)
    assert all(strategy.get_position(asset) is None for asset, _, _ in legs)
    assert strategy.get_position(Asset(symbol)) is None
    assert strategy.get_position(index_asset) is None
    # 5,000 + 200 credit - 500 (index 92 is below both puts: the 5 wide put side pays its width)
    assert float(strategy.get_cash()) == pytest.approx(4_700.0)
