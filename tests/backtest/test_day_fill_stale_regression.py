"""Regression test for https://github.com/Lumiwealth/lumibot/issues/1175.

With PolygonDataBacktesting and sleeptime="1D", once the minute frame was
loaded for an asset (e.g. via get_last_price()), market orders filled at the
previous trading day's 04:00 pre-market price instead of the current day's
price. The day-timestep fill priced off minute data went stale because the
minute->day resample drops the current (partial) session, so the fill landed
on the previous session's resampled bar.

polygon_helper.get_price_data_from_polygon is mocked, so this test needs no
network access and no API key.
"""

import datetime as dt

import pandas as pd
import pytest
import pytz

from lumibot.backtesting import PolygonDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.tools import polygon_helper

ET = pytz.timezone("America/New_York")

# The stale print the bug filled at: previous day's 04:00 pre-market price.
STALE_SENTINEL = 999.0
TODAY_OPEN = 101.0


def _make_day_df():
    idx = pd.DatetimeIndex(
        [ET.localize(dt.datetime(2026, 8, d)) for d in (3, 4, 5, 6)],
        name="datetime",
    )
    closes = [100.0, TODAY_OPEN, 102.0, 103.0]
    return pd.DataFrame(
        {"open": closes, "high": closes, "low": closes, "close": closes, "volume": [1000] * 4},
        index=idx,
    )


def _make_minute_df():
    rows = []
    for day, base in ((3, 100.0), (4, TODAY_OPEN), (5, 102.0), (6, 103.0)):
        cur = ET.localize(dt.datetime(2026, 8, day, 4, 0))
        end = ET.localize(dt.datetime(2026, 8, day, 16, 0))
        while cur < end:
            price = base
            if day == 3 and cur.hour == 4 and cur.minute == 0:
                price = STALE_SENTINEL  # the stale print the bug filled at
            rows.append((cur, price, price, price, price, 10))
            cur += dt.timedelta(minutes=1)
    idx = pd.DatetimeIndex([r[0] for r in rows], name="datetime")
    return pd.DataFrame(
        {
            "open": [r[1] for r in rows],
            "high": [r[2] for r in rows],
            "low": [r[3] for r in rows],
            "close": [r[4] for r in rows],
            "volume": [r[5] for r in rows],
        },
        index=idx,
    )


DAY_DF = _make_day_df()
MINUTE_DF = _make_minute_df()


def _fake_get_price_data_from_polygon(api_key, asset, start, end, timespan="minute", quote_asset=None, **kwargs):
    if timespan == "day":
        return DAY_DF.copy()
    return MINUTE_DF.copy()


class _StaleFillProbe(Strategy):
    fills = []

    def initialize(self):
        self.sleeptime = "1D"
        self.set_market("NYSE")
        self.bought = False

    def on_trading_iteration(self):
        now = self.get_datetime()
        a = Asset("PLTR")
        self.get_last_price(a)  # <-- loads the minute frame (the trigger in #1175)
        if not self.bought and now.date() == dt.date(2026, 8, 4):
            self.submit_order(self.create_order(a, 100, "buy"))
            self.bought = True

    def on_filled_order(self, position, order, price, quantity, multiplier):
        type(self).fills.append(float(price))


def test_market_fill_does_not_use_stale_premarket_price(monkeypatch):
    """A market order must not fill at the previous day's 04:00 pre-market print
    once the minute frame is loaded (issue #1175)."""
    monkeypatch.setenv("IS_BACKTESTING", "true")
    monkeypatch.setattr(
        polygon_helper,
        "get_price_data_from_polygon",
        _fake_get_price_data_from_polygon,
    )
    _StaleFillProbe.fills = []

    _StaleFillProbe.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=dt.datetime(2026, 8, 3),
        backtesting_end=dt.datetime(2026, 8, 6),
        budget=100000,
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        polygon_api_key="test",
        show_progress_bar=False,
        quiet_logs=True,
        save_tearsheet=False,
    )

    assert len(_StaleFillProbe.fills) == 1
    assert _StaleFillProbe.fills[0] == pytest.approx(TODAY_OPEN)
