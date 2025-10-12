from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import polars as pl
import pytest

from lumibot.backtesting.yahoo_backtesting_polars import YahooDataBacktestingPolars
from lumibot.entities import Asset


def _yahoo_frame(rows: int = 6) -> pl.DataFrame:
    base = datetime(2024, 12, 1, tzinfo=timezone.utc)
    datetimes = [base + timedelta(days=i) for i in range(rows)]
    return pl.DataFrame(
        {
            "datetime": datetimes,
            "open": [50 + i for i in range(rows)],
            "high": [51 + i for i in range(rows)],
            "low": [49 + i for i in range(rows)],
            "close": [50.5 + i for i in range(rows)],
            "volume": [1000 + i * 10 for i in range(rows)],
            "dividend": [0.0] * rows,
        }
    )


@pytest.fixture(autouse=True)
def stub_yahoo_helper(monkeypatch):
    helper = MagicMock()
    helper.get_symbol_data_optimized.return_value = _yahoo_frame(12)
    monkeypatch.setattr(
        "lumibot.backtesting.yahoo_backtesting_polars.YahooHelperPolarsOptimized",
        helper,
    )
    yield


def test_yahoo_polars_returns_bars():
    start = datetime(2024, 11, 1, tzinfo=timezone.utc)
    end = datetime(2024, 12, 31, tzinfo=timezone.utc)
    backtester = YahooDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        auto_adjust=False,
    )

    asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    backtester._store_data(asset, _yahoo_frame(12))
    backtester._update_datetime(start + timedelta(days=10))
    bars = backtester.get_historical_prices(asset, length=5, timestep="day", return_polars=True)

    assert bars is not None
    assert isinstance(bars.polars_df, pl.DataFrame)
