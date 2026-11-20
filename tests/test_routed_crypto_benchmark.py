"""A crypto-only routed backtest never fetches its benchmark through the stock data route.

Why: the default benchmark is the stock ticker SPY. In a routed backtest that only trades
crypto, SPY daily bars went through the stock route (a broker history line). That fetch is
cold for a crypto strategy and can sit for a long time, holding the whole run near the end.
A bare benchmark name that matches one of the strategy's own coins ("BTC") was also read as
a stock. Stock strategies keep using the stock route unchanged.
"""

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.entities import Asset
import lumibot.strategies._strategy as strategy_module
from lumibot.strategies._strategy import _Strategy


class _RecordingSource(RoutedBacktestingPandas):
    def __init__(self, data_store_keys):  # noqa: D401 - test double, skips the real init
        self._data_store = {k: None for k in data_store_keys}
        self.calls = []

    def get_historical_prices_between_dates(self, asset, *args, **kwargs):
        self.calls.append(asset)
        return None


class _Logger:
    def __init__(self):
        self.lines = []

    def info(self, msg, *a):
        self.lines.append(msg % a if a else msg)

    warning = error = info


def _fake_strategy(benchmark, traded_assets, quote=None):
    usdt = quote or Asset("USDT", asset_type="crypto")
    source = _RecordingSource([(a, usdt) for a in traded_assets])
    orders = [SimpleNamespace(asset=a) for a in traded_assets]
    broker = SimpleNamespace(data_source=source, get_all_orders=lambda: orders)
    fake = SimpleNamespace(
        is_backtesting=True,
        _benchmark_asset=benchmark,
        _backtesting_start=datetime(2026, 10, 1),
        _backtesting_end=datetime(2026, 11, 1),
        broker=broker,
        logger=_Logger(),
        _quote_asset=usdt,
        _sleeptime="30M",
        _benchmark_returns_df="unset",
    )
    return fake, source


@pytest.fixture
def public_returns(monkeypatch):
    calls = []

    def fake_returns(symbol, start, end):
        calls.append(symbol)
        idx = pd.date_range("2026-10-01", periods=3, freq="D", tz="America/New_York")
        return pd.DataFrame({"return": [0.0, 0.01, -0.01]}, index=idx)

    monkeypatch.setattr(strategy_module, "get_symbol_returns", fake_returns)
    return calls


def test_default_stock_benchmark_skips_stock_route_for_crypto_only_backtest(public_returns):
    btc_perp = Asset("BTCUSDT", asset_type="crypto_future")
    fake, source = _fake_strategy("SPY", [btc_perp])

    _Strategy._dump_benchmark_stats(fake)

    assert all(getattr(a, "asset_type", None) != "stock" for a in source.calls), source.calls
    assert public_returns == ["SPY"]
    assert fake._benchmark_returns_df is not None


def test_bare_coin_name_benchmark_is_the_coin_not_a_stock(public_returns):
    btc = Asset("BTC", asset_type="crypto")
    fake, source = _fake_strategy("BTC", [btc])

    _Strategy._dump_benchmark_stats(fake)

    assert source.calls, "the coin benchmark should be read from the routed crypto data"
    first = source.calls[0]
    assert getattr(first, "asset_type", None) == "crypto"
    assert public_returns == []


def test_stock_strategy_still_uses_stock_route(public_returns):
    spy_holder = Asset("QQQ", asset_type="stock")
    fake, source = _fake_strategy("SPY", [spy_holder], quote=Asset("USD", asset_type="forex"))

    _Strategy._dump_benchmark_stats(fake)

    assert [getattr(a, "symbol", None) for a in source.calls] == ["SPY"]
    assert source.calls[0].asset_type == "stock"
