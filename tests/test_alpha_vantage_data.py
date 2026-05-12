from __future__ import annotations

import pandas as pd
import pytest

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.data_sources.alpha_vantage_data import AlphaVantageData
from lumibot.entities import Asset


def _sample_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [1000, 1100, 1200],
        },
        index=pd.date_range("2024-01-02", periods=3, freq="D", tz=LUMIBOT_DEFAULT_PYTZ),
    )


def test_alpha_vantage_historical_prices_uses_in_memory_cache() -> None:
    asset = Asset("ALPHATEST")
    data_source = AlphaVantageData()
    data_source._data_store[asset] = _sample_ohlcv()
    data_source._datetime = data_source._data_store[asset].index[-1]

    bars = data_source.get_historical_prices(asset, 2, timestep="day")

    assert bars is not None
    assert len(bars.df) == 2
    assert bars.df["close"].tolist() == [101.5, 102.5]


def test_alpha_vantage_last_price_uses_cached_close() -> None:
    asset = Asset("ALPHATEST")
    data_source = AlphaVantageData()
    data_source._data_store[asset] = _sample_ohlcv()
    data_source._datetime = data_source._data_store[asset].index[-1]

    assert data_source.get_last_price(asset) == 102.5


def test_alpha_vantage_requires_api_key_for_uncached_download(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    data_source = AlphaVantageData()

    with pytest.raises(ValueError, match="API_KEY"):
        data_source.get_historical_prices(Asset("ALPHATEST"), 1, timestep="day")
