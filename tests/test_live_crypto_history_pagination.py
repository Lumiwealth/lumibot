import datetime
import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.data_sources.bitunix_data import BitunixData
from lumibot.data_sources.ccxt_data import CcxtData
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy

_INTERVAL_MS = {
    "1m": 60_000,
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
}


class _CappedBitunixClient:
    """Model Bitunix's 200-candle cap and timestamp-window pagination."""

    def __init__(self, end: pd.Timestamp):
        self.end_ms = int(end.timestamp() * 1000)
        self.calls = []

    def get_kline(
        self,
        symbol,
        interval,
        start_time=None,
        end_time=None,
        limit=None,
    ):
        self.calls.append(
            {
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
            }
        )
        step = _INTERVAL_MS[interval]
        effective_limit = min(limit or 100, 200)
        upper = min(end_time if end_time is not None else self.end_ms, self.end_ms)
        lower = start_time if start_time is not None else upper - (effective_limit + 1) * step
        first = ((lower // step) + 1) * step
        timestamps = list(range(first, upper, step))[:effective_limit]
        return {
            "code": 0,
            "data": [
                {
                    "time": timestamp,
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": "100",
                    "baseVol": "1",
                }
                for timestamp in timestamps
            ],
        }


def _bitunix_source(end: pd.Timestamp):
    source = BitunixData.__new__(BitunixData)
    source.name = "bitunix"
    source.tzinfo = datetime.timezone.utc
    source.client_symbols = set()
    source.client = _CappedBitunixClient(end)
    source.get_datetime = lambda: end.to_pydatetime()
    return source


def test_bitunix_paginates_past_exchange_limit():
    end = pd.Timestamp("2026-09-04T12:00:00Z")
    source = _bitunix_source(end)
    asset = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)

    bars = source.get_historical_prices(asset, length=450, timestep="minute")

    assert len(bars) == 450
    assert len(source.client.calls) >= 3
    assert all(call["limit"] <= 200 for call in source.client.calls)
    assert all(call["start_time"] is not None for call in source.client.calls)
    assert all(call["end_time"] is not None for call in source.client.calls)


@pytest.mark.parametrize(
    ("timestep", "required", "native_interval"),
    [
        ("15m", 20, "15m"),
        ("1h", 10, "1h"),
    ],
)
def test_live_strategy_prefers_bitunix_native_interval(timestep, required, native_interval):
    end = pd.Timestamp("2026-09-04T12:00:00Z")
    source = _bitunix_source(end)
    strategy = Strategy.__new__(Strategy)
    strategy.logger = logging.getLogger(__name__)
    strategy._logged_get_historical_prices_assets = set()
    strategy.is_backtesting = False
    strategy.broker = SimpleNamespace(
        data_source=source,
        option_source=None,
        IS_BACKTESTING_BROKER=False,
        quote_assets=set(),
    )
    strategy.quote_asset = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    asset = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)

    bars = strategy.get_historical_prices(asset, required, timestep=timestep)

    assert len(bars) >= required
    assert {call["interval"] for call in source.client.calls} == {native_interval}


class _SingleCandleCcxtApi:
    """Return one inclusive candle per call to expose a non-advancing cursor."""

    has = {"fetchOHLCV": True}
    markets = {"BTC/USD": {}}

    def __init__(self, end: datetime.datetime):
        self.end_ms = self.parse8601(end.strftime("%Y-%m-%d %H:%M:%S"))
        self.calls = []

    @staticmethod
    def parse8601(value):
        return int(pd.Timestamp(value).timestamp() * 1000)

    def fetch_ohlcv(self, symbol, freq, since, limit, params):
        self.calls.append(since)
        if since > self.end_ms:
            return []
        return [[since, 100, 101, 99, 100, 1]]


def test_ccxt_live_pagination_advances_past_inclusive_last_candle():
    end = datetime.datetime(2026, 9, 4, 12, 0)
    api = _SingleCandleCcxtApi(end)
    source = CcxtData.__new__(CcxtData)
    source.api = api
    source._ensure_markets_loaded = lambda: None

    frame = source.get_barset_from_api(api, "BTC/USD", "1m", limit=3, end=end)

    assert len(frame) == 3
    assert frame.index[-1] == pd.Timestamp(end)
    assert all(
        later - earlier == 60_000
        for earlier, later in zip(api.calls, api.calls[1:])
    )
