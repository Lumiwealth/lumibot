"""Provider epochs identify instants, independent of strategy/host timezone."""

import datetime as dt
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.data_sources.schwab_data import SchwabData
from lumibot.data_sources.ccxt_data import CcxtData
from lumibot.entities import Asset


@pytest.mark.parametrize("instant", [
    "2026-10-16T13:30:00Z",  # Friday 09:30 EDT, before a 13:06 account mark
    "2026-01-16T14:30:00Z",  # Friday 09:30 EST
    "2026-10-16T00:05:00Z",  # previous local date
    "2026-11-01T05:30:00Z",  # first repeated 01:30
    "2026-11-01T06:30:00Z",  # second repeated 01:30
])
def test_schwab_history_preserves_provider_instant(instant):
    stamp = pd.Timestamp(instant)
    source = SchwabData(auto_create_client=False)
    response = {"candles": [{"datetime": stamp.value // 1_000_000,
        "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]}
    source.client = SimpleNamespace(get_price_history_every_day=lambda *a, **k: response)

    bars = source.get_historical_prices(Asset("SPY"), 1, "day")

    assert bars is not None
    assert bars.df.index[0] == stamp
    assert bars.df.index[0].tzinfo is not None
    assert source.convert_epoch_ms_to_datetime(stamp.value // 1_000_000) == bars.df.index[0]


@pytest.mark.parametrize("end", [
    dt.datetime(2026, 10, 16, 13, 6, tzinfo=dt.timezone.utc),
    pd.Timestamp("2026-10-16T09:06:00-04:00").to_pydatetime(),
    dt.datetime(2026, 10, 16, 13, 6),  # legacy naive input denotes UTC
])
@pytest.mark.parametrize("shared_cache", [False, True])
def test_ccxt_history_preserves_epoch_and_cutoff(end, shared_cache, tmp_path, monkeypatch):
    from threading import Lock
    from lumibot import constants
    from lumibot.tools import backtest_cache, parquet_series_cache, shared_bar_cache

    monkeypatch.setenv("LUMIBOT_SHARED_BARS", str(shared_cache).lower())
    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: backtest_cache.BacktestCacheManager(None))
    shared_bar_cache._reset_process_memory()

    source = object.__new__(CcxtData)
    source._markets_loaded = True
    source._markets_lock = Lock()
    first = pd.Timestamp("2026-10-16T13:00:00Z")
    after = pd.Timestamp("2026-10-16T13:10:00Z")
    requests = []

    def fetch(symbol, freq, since, limit, params):
        requests.append(since)
        return [[t.value // 1_000_000, 1, 1, 1, 1, 1] for t in (first, after)]

    source.api = SimpleNamespace(
        has={"fetchOHLCV": True}, markets={"BTC/USD": {}}, fetch_ohlcv=fetch,
        parse8601=lambda value: int(pd.Timestamp(value, tz="UTC").timestamp() * 1000),
    )
    frame = source.get_barset_from_api(source.api, "BTC/USD", "5m", limit=1, end=end)

    assert list(frame.index) == [first]
    assert frame.index.tz is not None
    # The shared cache deliberately overlaps one earlier bar when filling a gap.
    lookback_minutes = 60 if shared_cache else 55
    assert requests[0] == int((pd.Timestamp("2026-10-16T13:06Z") - pd.Timedelta(minutes=lookback_minutes)).timestamp() * 1000)


@pytest.mark.parametrize("instant", ["2026-01-16T14:30Z", "2026-10-16T13:30Z"])
def test_ibkr_rest_history_preserves_epoch(instant):
    from lumibot.data_sources.interactive_brokers_rest_data import InteractiveBrokersRESTData

    source = object.__new__(InteractiveBrokersRESTData)
    source.ping_iserver = lambda: None
    source.get_conid_from_asset = lambda **kwargs: 1
    source.base_url = "https://synthetic.invalid"
    stamp = pd.Timestamp(instant)
    source.get_from_endpoint = lambda *args: {"data": [{"t": stamp.value // 1_000_000,
        "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}

    bars = source.get_historical_prices(Asset("SPY"), 1, "minute")

    assert bars.df.index[0] == stamp


@pytest.mark.parametrize("instant", ["2026-01-16T09:30", "2026-10-16T09:30"])
def test_tradier_local_wall_time_is_not_reinterpreted_as_utc(instant, monkeypatch):
    from lumibot.data_sources import tradier_data

    source = tradier_data.TradierData(account_number="synthetic", access_token="synthetic")
    frame = pd.DataFrame({"open": [1], "high": [1], "low": [1], "close": [1], "volume": [1]},
                         index=pd.DatetimeIndex([instant]))
    source.tradier = SimpleNamespace(market=SimpleNamespace(get_timesales=lambda *a, **k: frame))
    monkeypatch.setattr(tradier_data, "_date_n_trading_days_from_date", lambda **kwargs: dt.date(2026, 1, 1))

    bars = source.get_historical_prices(Asset("SPY"), 1, "minute")

    assert bars.df.index[0] == pd.Timestamp(instant, tz="America/New_York")
