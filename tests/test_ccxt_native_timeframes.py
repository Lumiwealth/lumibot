"""Live CCXT reads ask the exchange for the bar size it already serves.

Nov 20, 2026: an hourly perpetual chart read through a CCXT broker connection asked for
1,220 hourly bars and LumiBot fetched 73,200 one-minute candles instead (about 244 requests,
164 seconds), because CcxtData did not say it serves hourly candles natively.
"""
from unittest.mock import MagicMock

import pandas as pd

from lumibot.data_sources.ccxt_data import CcxtData
from lumibot.strategies.strategy import _data_source_serves_timestep_natively


def _source(timeframes):
    source = CcxtData.__new__(CcxtData)
    source.api = MagicMock()
    source.api.timeframes = timeframes
    return source


PHEMEX_LIKE = {"1m": "60", "5m": "300", "15m": "900", "1h": "3600", "4h": "14400", "1d": "86400"}


def test_hourly_and_multi_minute_bars_are_native_when_the_exchange_lists_them():
    source = _source(PHEMEX_LIKE)
    for timestep in ("hour", "1h", "4 hours", "15 minutes", "15m", "15minute"):
        assert _data_source_serves_timestep_natively(source, timestep), timestep


def test_intervals_the_exchange_lacks_stay_on_the_minute_path():
    source = _source(PHEMEX_LIKE)
    assert not _data_source_serves_timestep_natively(source, "37 minutes")
    assert not _data_source_serves_timestep_natively(source, "2 hours")


def test_native_read_pages_the_exchange_interval_not_minutes():
    source = _source(PHEMEX_LIKE)
    seen = {}

    def barset(api, symbol, freq, limit=None, end=None):
        seen["freq"], seen["limit"] = freq, limit
        index = pd.date_range("2026-11-01", periods=limit, freq="h", tz="UTC")
        return pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0}, index=index)

    source.get_barset_from_api = barset
    source.get_market = lambda asset, quote=None: {"symbol": "SOL/USDT:USDT"}
    source._pull_source_bars(["SOL"], 1220, timestep="hour")
    assert seen == {"freq": "1h", "limit": 1220}
