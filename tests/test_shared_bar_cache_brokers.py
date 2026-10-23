"""Every broker's live history path reads through the one shared bar cache.

Each case drives the real data-source method with a fake provider client that
serves deterministic bars and records every provider call. A repeated read of
settled history must make zero provider calls; a repeated live read must fetch
only the still-open tail. Returned bars must match an uncached read exactly.
"""

from __future__ import annotations

import datetime as dt
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
    shared_bar_cache._Store._memory.clear()
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


def _read(source, asset, length, timestep, timeshift=None):
    if isinstance(asset, tuple):
        return source.get_historical_prices(asset[0], length, timestep, timeshift=timeshift, quote=asset[1])
    return source.get_historical_prices(asset, length, timestep, timeshift=timeshift)


@pytest.mark.parametrize("broker", sorted(BROKERS))
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


@pytest.mark.parametrize("broker", sorted(BROKERS))
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


@pytest.mark.parametrize("broker", sorted(BROKERS))
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


@pytest.mark.parametrize("broker", sorted(BROKERS))
def test_different_connections_do_not_share_bars_by_default(broker):
    make, asset = BROKERS[broker]
    first_source, _ = make()
    _read(first_source, asset, 30, "day", dt.timedelta(days=40))
    other = {"schwab": "schwab-account-2", "tradier": "VA000002", "ibkr": "DU0000002", "ccxt": "coinbase"}[broker]
    second_source, provider = make(other)
    _read(second_source, asset, 30, "day", dt.timedelta(days=40))
    assert provider.calls, f"{broker}: another connection's bars were reused"


@pytest.mark.parametrize("broker", sorted(BROKERS))
def test_history_cache_measurements_reach_the_caller(broker):
    make, asset = BROKERS[broker]
    source, _ = make()
    _read(source, asset, 30, "day", dt.timedelta(days=40))
    bars = _read(source, asset, 30, "day", dt.timedelta(days=40))
    stats = bars.df.attrs.get("historyCache") or {}
    assert stats.get("kind") == "lumibot_shared_bars" and stats.get("hit") is True
