"""Every broker's live history path reads through the one shared bar cache.

Each case drives the real data-source method with a fake provider client that
serves deterministic bars and records every provider call. A repeated read of
settled history must make zero provider calls; a repeated live read must fetch
only the still-open tail. Returned bars must match an uncached read exactly.
"""

from __future__ import annotations

import datetime as dt
import sys
import threading
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import shared_bar_cache


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    """Local cache folder per test; never touch a developer's real S3 cache."""
    from lumibot import constants
    from lumibot.tools import parquet_series_cache
    from lumibot.tools.backtest_cache import BacktestCacheManager

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: BacktestCacheManager(None))
    monkeypatch.delenv("LUMIBOT_SHARED_BARS", raising=False)
    monkeypatch.delenv("LUMIBOT_SHARED_BARS_SCOPE", raising=False)
    shared_bar_cache._reset_process_memory()
    yield tmp_path


def _price(stamp: pd.Timestamp) -> float:
    return 100.0 + (stamp.value // 60_000_000_000 % 997) / 10.0


def synthetic_bars(start, end, freq: str, *, weekdays_only=False) -> pd.DataFrame:
    """Deterministic real-looking OHLCV for every bar start in [start, end]."""
    start, end = pd.Timestamp(start).tz_convert("UTC"), pd.Timestamp(end).tz_convert("UTC")
    if freq == "1D":
        index = pd.date_range(start.normalize(), end, freq="1D", tz="UTC") + pd.Timedelta(hours=5)
    else:
        index = pd.date_range(start.ceil(freq), end, freq=freq, tz="UTC")
    index = index[(index >= start) & (index <= end)]
    if weekdays_only:
        index = index[index.dayofweek < 5]
    close = np.array([_price(s) for s in index])
    return pd.DataFrame(
        {"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 1000.0}, index=index
    )


# --------------------------------------------------------------------------- Schwab


class FakeSchwabClient:
    class PriceHistory:
        FrequencyType = SimpleNamespace(MINUTE="minute", DAILY="daily", WEEKLY="weekly", MONTHLY="monthly")

        @staticmethod
        def Frequency(value):  # noqa: N802 - mirrors the schwab-py enum constructor
            return value

    def __init__(self):
        self.calls = []

    @staticmethod
    def _candles(frame):
        return {
            "candles": [
                {
                    "datetime": int(stamp.value // 1_000_000),
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                }
                for stamp, row in frame.iterrows()
            ]
        }

    def get_price_history_every_day(self, symbol, start_datetime, end_datetime, need_extended_hours_data=True):
        self.calls.append((pd.Timestamp(start_datetime), pd.Timestamp(end_datetime)))
        return self._candles(synthetic_bars(start_datetime, end_datetime, "1D", weekdays_only=True))

    def get_price_history(
        self, symbol, frequency_type, frequency, start_datetime, end_datetime, need_extended_hours_data=True
    ):
        self.calls.append((pd.Timestamp(start_datetime), pd.Timestamp(end_datetime)))
        return self._candles(synthetic_bars(start_datetime, end_datetime, f"{frequency}min"))


def schwab_source(scope="schwab-account-1"):
    from lumibot.data_sources.schwab_data import SchwabData

    client = FakeSchwabClient()
    source = SchwabData(client=client)
    source.shared_bars_scope = shared_bar_cache.credential_scope("schwab", scope)
    return source, client


# --------------------------------------------------------------------------- Tradier


class FakeTradierMarket:
    def __init__(self, tz):
        self.calls = []
        self.tz = tz

    def get_historical_quotes(self, symbol, interval, start_date, end_date, session_filter="all"):
        self.calls.append((pd.Timestamp(start_date), pd.Timestamp(end_date)))
        # Like the real API: daily rows are dates, selected by calendar date in the
        # caller's time zone (the request's time of day does not matter).
        first = pd.Timestamp(start_date).tz_convert(self.tz).date()
        last = pd.Timestamp(end_date).tz_convert(self.tz).date()
        days = [day.date() for day in pd.bdate_range(first, last)]
        close = np.array([_price(pd.Timestamp(day)) for day in days])
        return pd.DataFrame(
            {"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 1000.0}, index=days
        )

    def get_timesales(self, symbol, interval, start_date, end_date, session_filter="all"):
        self.calls.append((pd.Timestamp(start_date), pd.Timestamp(end_date)))
        frame = synthetic_bars(start_date, end_date, f"{interval}min")
        frame.index = frame.index.tz_convert(self.tz)
        return frame


def tradier_source(account="VA000001"):
    from lumibot.data_sources.tradier_data import TradierData

    source = TradierData(account_number=account, access_token="synthetic-token", paper=True)
    source.tradier = SimpleNamespace(market=FakeTradierMarket(source.tzinfo))
    return source, source.tradier.market


# --------------------------------------------------------------------------- Interactive Brokers (Client Portal)


class FakeIbkrEndpoint:
    def __init__(self):
        self.calls = []

    def __call__(self, url, description=None, **kwargs):
        from urllib.parse import parse_qs, urlparse

        query = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        end = pd.Timestamp(dt.datetime.strptime(query["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        period, bar = query["period"], query["bar"]
        units = {"min": "min", "h": "h", "d": "D", "w": "W"}
        number = int("".join(ch for ch in period if ch.isdigit()))
        unit = period[len(str(number)) :]
        start = end - pd.Timedelta(number, units[unit] if unit != "w" else "W")
        freq = {"1min": "1min", "5min": "5min", "1h": "1h", "1d": "1D"}[bar]
        self.calls.append((start, end))
        frame = synthetic_bars(start, end, freq, weekdays_only=freq == "1D")
        return {
            "data": [
                {"t": int(s.value // 1_000_000), "o": r.open, "h": r.high, "l": r.low, "c": r.close, "v": r.volume}
                for s, r in frame.iterrows()
            ]
        }


def ibkr_source(account="DU0000001"):
    from lumibot.data_sources.interactive_brokers_rest_data import InteractiveBrokersRESTData

    source = InteractiveBrokersRESTData.__new__(InteractiveBrokersRESTData)
    endpoint = FakeIbkrEndpoint()
    source.base_url = "https://gateway.invalid/v1/api"
    source.account_id = account
    source.ping_iserver = lambda: None
    source.get_conid_from_asset = lambda asset: 265598
    source.get_from_endpoint = endpoint
    source.shared_bars_scope = shared_bar_cache.credential_scope("ibkr", account)
    return source, endpoint


# --------------------------------------------------------------------------- CCXT crypto


class FakeCcxtApi:
    has = {"fetchOHLCV": True}
    markets = {"BTC/USD": {"symbol": "BTC/USD"}}

    def __init__(self):
        self.calls = []

    @staticmethod
    def parse8601(text):
        return int(pd.Timestamp(text, tz="UTC").value // 1_000_000)

    def fetch_ohlcv(self, symbol, freq, since=None, limit=300, params=None):
        step = {"1m": pd.Timedelta(minutes=1), "1d": pd.Timedelta(days=1)}[freq]
        start = pd.Timestamp(since, unit="ms", tz="UTC")
        now = pd.Timestamp.now(tz="UTC")
        stop = min(start + step * (limit - 1), now)
        self.calls.append((start, stop))
        frame = synthetic_bars(start, stop, "1min" if freq == "1m" else "1D")
        return [[int(s.value // 1_000_000), r.open, r.high, r.low, r.close, r.volume] for s, r in frame.iterrows()]


def ccxt_source(exchange="kraken"):
    from lumibot.data_sources.ccxt_data import CcxtData

    source = CcxtData.__new__(CcxtData)
    source.api = FakeCcxtApi()
    source.config = {"exchange_id": exchange, "apiKey": "synthetic"}
    source._markets_loaded = True
    source._markets_lock = threading.Lock()
    source.shared_bars_scope = shared_bar_cache.credential_scope("ccxt", exchange, "synthetic")
    return source, source.api


# --------------------------------------------------------------------------- the contract

BROKERS = {
    "schwab": (schwab_source, Asset("SPY")),
    "tradier": (tradier_source, Asset("SPY")),
    "ibkr": (ibkr_source, Asset("SPY")),
    "ccxt": (ccxt_source, (Asset("BTC", asset_type="crypto"), Asset("USD", asset_type="forex"))),
}


# Interactive Brokers Client Portal reads bypass the cache (see interactive_brokers_rest_data.py).
CACHED_BROKERS = {name: value for name, value in BROKERS.items() if name != "ibkr"}


def test_ibkr_client_portal_reads_always_go_to_the_gateway():
    source, provider = ibkr_source()
    first = _read(source, Asset("SPY"), 30, "day", dt.timedelta(days=40))
    second = _read(source, Asset("SPY"), 30, "day", dt.timedelta(days=40))
    assert len(provider.calls) == 2
    pd.testing.assert_frame_equal(first.df[["open", "close"]], second.df[["open", "close"]], check_freq=False)


def _read(source, asset, length, timestep, timeshift=None):
    if isinstance(asset, tuple):
        return source.get_historical_prices(asset[0], length, timestep, timeshift=timeshift, quote=asset[1])
    return source.get_historical_prices(asset, length, timestep, timeshift=timeshift)


@pytest.mark.parametrize("broker", sorted(CACHED_BROKERS))
def test_repeated_settled_daily_history_makes_zero_provider_calls(broker):
    make, asset = BROKERS[broker]
    source, provider = make()
    shift = dt.timedelta(days=40)
    first = _read(source, asset, 30, "day", shift)
    assert first is not None and len(first.df) > 0
    calls = len(provider.calls)
    second = _read(source, asset, 30, "day", shift)
    assert len(provider.calls) == calls, f"{broker}: repeated settled read went back to the provider"
    pd.testing.assert_frame_equal(
        first.df[["open", "high", "low", "close", "volume"]],
        second.df[["open", "high", "low", "close", "volume"]],
        check_freq=False,
    )


@pytest.mark.parametrize("broker", sorted(CACHED_BROKERS))
def test_repeated_live_minute_read_fetches_only_the_open_tail(broker):
    make, asset = BROKERS[broker]
    source, provider = make()
    first = _read(source, asset, 300, "minute")
    assert first is not None and len(first.df) > 0
    calls = len(provider.calls)
    second = _read(source, asset, 300, "minute")
    later = provider.calls[calls:]
    assert later, f"{broker}: the still-open tail must always be read again"
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=30)
    assert all(start >= cutoff for start, _ in later), f"{broker}: re-downloaded settled bars {later}"
    common = first.df.index.intersection(second.df.index)
    assert len(common) > 200
    pd.testing.assert_frame_equal(
        first.df.loc[common, ["open", "close"]], second.df.loc[common, ["open", "close"]], check_freq=False
    )


@pytest.mark.parametrize("broker", sorted(CACHED_BROKERS))
def test_cached_read_matches_an_uncached_read(broker, monkeypatch):
    make, asset = BROKERS[broker]
    source, _ = make()
    shift = dt.timedelta(days=10)
    _read(source, asset, 30, "day", shift)
    cached = _read(source, asset, 30, "day", shift)
    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "false")
    plain_source, _ = make()
    plain = _read(plain_source, asset, 30, "day", shift)
    pd.testing.assert_frame_equal(
        cached.df[["open", "high", "low", "close"]], plain.df[["open", "high", "low", "close"]], check_freq=False
    )


@pytest.mark.parametrize("broker", sorted(CACHED_BROKERS))
def test_different_connections_do_not_share_bars_by_default(broker):
    make, asset = BROKERS[broker]
    first_source, _ = make()
    _read(first_source, asset, 30, "day", dt.timedelta(days=40))
    other = {"schwab": "schwab-account-2", "tradier": "VA000002", "ibkr": "DU0000002", "ccxt": "coinbase"}[broker]
    second_source, provider = make(other)
    _read(second_source, asset, 30, "day", dt.timedelta(days=40))
    assert provider.calls, f"{broker}: another connection's bars were reused"


@pytest.mark.parametrize("broker", sorted(CACHED_BROKERS))
def test_history_cache_measurements_reach_the_caller(broker):
    make, asset = BROKERS[broker]
    source, _ = make()
    _read(source, asset, 30, "day", dt.timedelta(days=40))
    bars = _read(source, asset, 30, "day", dt.timedelta(days=40))
    stats = bars.df.attrs.get("historyCache") or {}
    assert stats.get("kind") == "lumibot_shared_bars" and stats.get("hit") is True


@pytest.mark.parametrize("broker", ["schwab", "tradier"])
def test_real_broker_quirks_still_return_bars(broker, monkeypatch):
    """Tradier SPY 2024-12-19 really has close 584.134452 below low 585.85. The old path returned it;
    the cache must too (Dev, Oct 23: the cached path returned no bars at all)."""
    make, asset = BROKERS[broker]
    source, provider = make()
    original = synthetic_bars

    def quirky(start, end, freq, *, weekdays_only=False):
        frame = original(start, end, freq, weekdays_only=weekdays_only)
        frame.iloc[::5, frame.columns.get_loc("close")] = frame["low"].iloc[::5] - 1.0
        return frame

    monkeypatch.setattr(sys.modules[__name__], "synthetic_bars", quirky)
    if broker == "tradier":
        original_quotes = provider.get_historical_quotes

        def quotes(*args, **kwargs):
            frame = original_quotes(*args, **kwargs)
            frame.iloc[::5, frame.columns.get_loc("close")] = frame["low"].iloc[::5] - 1.0
            return frame

        provider.get_historical_quotes = quotes
    bars = _read(source, asset, 30, "day", dt.timedelta(days=40))
    assert bars is not None and len(bars.df) > 10
    assert (bars.df["close"] < bars.df["low"]).any(), "the provider's own bar must come back unchanged"
    calls = len(provider.calls)
    _read(source, asset, 30, "day", dt.timedelta(days=40))
    assert len(provider.calls) == calls, "quirky bars must be cached like any other"


def test_adapters_return_the_providers_own_timestamps_with_the_cache_on(monkeypatch):
    """Bot Manager's scheduled smoke contract (Dev, Oct 23 2026): installed adapters given synthetic
    transport answers must return exactly the provider's bar, even when it lies outside a calendar
    window (IBKR answers "the last N bars", e.g. the 16:00 bar at night). 4.6.26 returned no bars."""
    monkeypatch.delenv("LUMIBOT_SHARED_BARS", raising=False)
    import pandas as pd

    # Actual installed adapters, synthetic transport responses, no broker calls.
    import datetime as dt
    from threading import Lock
    from types import SimpleNamespace
    from unittest.mock import patch
    from lumibot.data_sources.schwab_data import SchwabData
    from lumibot.data_sources.ccxt_data import CcxtData
    from lumibot.data_sources.interactive_brokers_rest_data import InteractiveBrokersRESTData
    from lumibot.data_sources import tradier_data
    from lumibot.entities import Asset

    for instant in ("2026-01-16T14:30Z", "2026-10-16T13:30Z", "2026-10-16T00:05Z"):
        stamp = pd.Timestamp(instant)
        source = SchwabData(auto_create_client=False)
        response = {"candles": [{"datetime": stamp.value // 1_000_000,
                    "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]}
        source.client = SimpleNamespace(get_price_history_every_day=lambda *a, **k: response)
        assert source.get_historical_prices(Asset("SPY"), 1, "day").df.index[0] == stamp
        rest = object.__new__(InteractiveBrokersRESTData)
        rest.ping_iserver = lambda: None
        rest.get_conid_from_asset = lambda **kwargs: 1
        rest.base_url = "https://synthetic.invalid"
        rest.get_from_endpoint = lambda *args: {"data": [{"t": stamp.value // 1_000_000,
            "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}
        assert rest.get_historical_prices(Asset("SPY"), 1, "minute").df.index[0] == stamp

    source = object.__new__(CcxtData)
    source._markets_loaded = True
    source._markets_lock = Lock()
    first, after = pd.Timestamp("2026-10-16T13:00Z"), pd.Timestamp("2026-10-16T13:10Z")
    source.api = SimpleNamespace(has={"fetchOHLCV": True}, markets={"BTC/USD": {}},
        fetch_ohlcv=lambda *args, **kwargs: [[t.value // 1_000_000, 1, 1, 1, 1, 1] for t in (first, after)],
        parse8601=lambda value: int(pd.Timestamp(value, tz="UTC").timestamp() * 1000))
    end = dt.datetime(2026, 10, 16, 13, 6, tzinfo=dt.timezone.utc)
    assert list(source.get_barset_from_api(source.api, "BTC/USD", "5m", limit=1, end=end).index) == [first]

    for local in ("2026-01-16T09:30", "2026-10-16T09:30"):
        source = tradier_data.TradierData(account_number="synthetic", access_token="synthetic")
        frame = pd.DataFrame({"open": [1], "high": [1], "low": [1], "close": [1], "volume": [1]},
                             index=pd.DatetimeIndex([local]))
        source.tradier = SimpleNamespace(market=SimpleNamespace(get_timesales=lambda *a, **k: frame))
        with patch.object(tradier_data, "_date_n_trading_days_from_date", return_value=dt.date(2026, 1, 1)):
            assert source.get_historical_prices(Asset("SPY"), 1, "minute").df.index[0] == pd.Timestamp(local, tz="America/New_York")
    print("installed broker timestamp contracts passed")
