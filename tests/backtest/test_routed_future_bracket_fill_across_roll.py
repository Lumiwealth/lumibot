"""A bracket order on a continuous future must fill on the dated contract across a roll.

The routed data source resolves a continuous future to the dated contract for fills.  The
provider pages history backward from the requested end, so a request that ends after the
contract's expiration comes back empty.  Asking a contract for bars up to the end of the
backtest therefore loaded nothing for every contract that expired inside the window, and the
entry stayed pending for the rest of the run (zero trades in a 12-month backtest).
"""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy

TARGET_POINTS = 10.0
STOP_POINTS = 5.0
BASE = 6000.0


def _session_minutes(start, end):
    """Weekday 1-minute bars with the one-hour evening maintenance break removed."""
    index = pd.date_range(start, end, freq="1min", tz=LUMIBOT_DEFAULT_PYTZ)
    return index[(index.dayofweek < 5) & (index.hour != 17)]


def _synthetic_minutes(start, end):
    """Flat overnight; each morning breaks out, then runs to the target (even days) or the stop."""
    index = _session_minutes(start, end)
    minute_of_day = index.hour * 60 + index.minute
    up_day = (index.dayofyear % 2) == 0
    close = np.full(len(index), BASE)
    ramp = (minute_of_day >= 630) & (minute_of_day < 645)            # 10:30 breakout, +8 points
    close[ramp] = BASE + 8.0 * (minute_of_day[ramp] - 629) / 15.0
    move = (minute_of_day >= 645) & (minute_of_day < 720)            # 10:45 to 12:00
    progress = (minute_of_day[move] - 645) / 75.0
    close[move] = BASE + 8.0 + np.where(up_day[move], 14.0, -30.0) * progress
    back = (minute_of_day >= 720) & (minute_of_day < 840)            # back to flat by 14:00
    peak = np.where(up_day[back], 22.0, -22.0)
    close[back] = BASE + peak * (1.0 - (minute_of_day[back] - 720) / 120.0)
    frame = pd.DataFrame({"open": close, "high": close + 0.25, "low": close - 0.25,
                          "close": close, "volume": 100.0}, index=index)
    frame["open"] = frame["close"].shift(1).fillna(BASE)
    frame["high"] = frame[["open", "close"]].max(axis=1) + 0.25
    frame["low"] = frame[["open", "close"]].min(axis=1) - 0.25
    return frame


def _resampled(frame, minutes):
    if minutes == 1:
        return frame
    return (frame.resample(f"{minutes}min")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna())


class _BracketBreakout(Strategy):
    def initialize(self):
        self.sleeptime = "15M"
        self.set_market("us_futures")

    def on_trading_iteration(self):
        asset = Asset(self.parameters["symbol"], asset_type=Asset.AssetType.CONT_FUTURE)
        now = self.get_datetime()
        if not ((9, 35) <= (now.hour, now.minute) < (15, 30)):
            return
        position = self.get_position(asset)
        if (position is not None and float(position.quantity) != 0) or self.get_orders(statuses="open"):
            return
        bars = self.get_historical_prices(asset, 21, "15 minutes")
        if bars is None or bars.df is None or len(bars.df) < 21:
            return
        price = float(bars.df["close"].iloc[-1])
        if price > float(bars.df["high"].iloc[:-1].max()):
            self.submit_order(self.create_order(
                asset, 1, "buy", order_class=Order.OrderClass.BRACKET,
                secondary_limit_price=price + TARGET_POINTS,
                secondary_stop_price=price - STOP_POINTS))


@pytest.mark.usefixtures("disable_datasource_override")
@pytest.mark.parametrize("symbol", ["ES", "MES", "NQ", "MNQ", "GC", "MGC", "CL"])
def test_bracket_entry_stop_and_target_fill_on_each_contract_across_a_roll(monkeypatch, tmp_path, symbol):
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    from lumibot.tools import ibkr_helper

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test-key")
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")

    start, end = datetime(2025, 11, 10), datetime(2026, 1, 16)
    minutes = _synthetic_minutes(start - timedelta(days=10), end + timedelta(days=2))
    requests = []

    def prices(**kwargs):
        asset, timestep = kwargs["asset"], str(kwargs["timestep"])
        begin, finish = kwargs["start_dt"], kwargs["end_dt"]
        requests.append((asset, timestep, begin, finish))
        expiration = getattr(asset, "expiration", None)
        if expiration is not None:
            after_last_day = LUMIBOT_DEFAULT_PYTZ.localize(
                datetime.combine(expiration + timedelta(days=1), datetime.min.time()))
            if finish > after_last_day:
                return pd.DataFrame()  # history pages backward: nothing exists after expiration
        quantity = int(timestep.replace("minute", "") or 1) if timestep.endswith("minute") else None
        assert quantity is not None, f"unexpected cadence {timestep}"
        frame = _resampled(minutes, quantity)
        return frame.loc[(frame.index >= begin) & (frame.index <= finish)].copy()

    monkeypatch.setattr(ibkr_helper, "get_price_data", prices)

    _, strategy = _BracketBreakout.run_backtest(
        RoutedBacktestingPandas, start, end,
        config={"backtesting_data_routing": {"default": "ibkr"}},
        parameters={"symbol": symbol}, budget=1_000_000,
        benchmark_asset=None, risk_free_rate=0, show_plot=False, show_tearsheet=False,
        show_indicators=False, save_tearsheet=False, show_progress_bar=False, quiet_logs=True,
    )

    filled = strategy.broker._filled_orders.get_list()
    by_contract = {}
    for order in filled:
        kinds = by_contract.setdefault(order.asset.expiration, set())
        if order.is_buy_order():
            kinds.add("entry")
        elif order.order_type == Order.OrderType.STOP:
            kinds.add("stop")
        elif order.order_type == Order.OrderType.LIMIT:
            kinds.add("target")
    dated = {expiration: kinds for expiration, kinds in by_contract.items() if expiration is not None}
    assert len(dated) >= 2, f"expected fills on contracts before and after the roll, got {by_contract}"
    for expiration, kinds in dated.items():
        assert kinds >= {"entry", "stop", "target"}, f"{symbol} {expiration}: only {sorted(kinds)} filled"
    # No request for a dated contract may end after that contract stopped trading.
    for asset, _, _, finish in requests:
        expiration = getattr(asset, "expiration", None)
        if expiration is not None:
            assert finish.date() <= expiration + timedelta(days=1)
