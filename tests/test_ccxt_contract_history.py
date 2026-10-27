"""Perpetual history retains contract identity through the real DuckDB cache."""

from datetime import datetime, timedelta
import shutil
from unittest.mock import Mock

import pytest

from lumibot.data_sources.ccxt_backtesting_data import CcxtBacktestingData
from lumibot.entities import Asset
from tests.test_ccxt_derivatives import derivatives
from tests.test_ccxt_unified_spot import make_broker
from tests.test_ccxt_store import _bars, _cache_without_live_exchange


@pytest.fixture
def contract_cache_directory(tmp_path):
    """Remove only this test's isolated, closed DuckDB files on every outcome."""
    directory = tmp_path / "contract-cache"
    directory.mkdir()
    try:
        yield directory
    finally:
        shutil.rmtree(directory)


@pytest.mark.parametrize("symbol", ["BTC/USDT", "BTC/USDT:USDT", "BTC/USDC:USDC", "BTC/USD:BTC"])
def test_history_preserves_spot_or_settlement_through_cache(contract_cache_directory, monkeypatch, derivatives, symbol):
    cache = _cache_without_live_exchange(contract_cache_directory, monkeypatch)
    cache.api = derivatives.api
    cache.exchange_id = "phemex"
    # Only the external candle response is substituted; the cache executes its
    # actual schema, SQL writes and reads in the task-owned temporary directory.
    def provider_candles(requested, timeframe, limit, start, end):
        assert requested == symbol
        return _bars((datetime(2026, 1, 1, 5, 0), 100, 102, 99, 101, 2),
                     (datetime(2026, 1, 1, 5, 1), 101, 103, 100, 102, 3))
    cache._get_barset_from_api = Mock(side_effect=provider_candles)
    monkeypatch.setattr("lumibot.data_sources.ccxt_backtesting_data.CcxtCacheDB", lambda *a, **kw: cache)
    start = datetime(2026, 1, 1)
    source = CcxtBacktestingData(datetime_start=start, datetime_end=start + timedelta(minutes=3), exchange_id="phemex")
    source._download_start_dt_prebuffer = 0
    source._update_datetime(source.datetime_start + timedelta(minutes=2))
    asset = derivatives.get_asset(symbol)
    quote = Asset(derivatives.api.markets[symbol]["quote"], "crypto")
    result = source._pull_source_symbol_bars((asset, quote), 2, "minute")
    assert result["close"].tolist() == [101, 102]
    assert f"{symbol}_1m" in source._data_store
    assert result.index[-1] + timedelta(minutes=1) <= source.get_datetime()
    window = source.get_historical_prices_between_dates(
        (asset, quote), start_date=source.datetime_start, end_date=source.get_datetime())
    assert window.df["close"].tolist() == [101, 102]
