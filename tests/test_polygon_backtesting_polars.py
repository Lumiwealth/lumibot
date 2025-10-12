from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import polars as pl
import pytest

from lumibot.backtesting.polygon_backtesting_polars import PolygonDataBacktestingPolars
from lumibot.entities import Asset


@pytest.fixture(autouse=True)
def stub_polygon_client(monkeypatch):
    monkeypatch.setattr(
        "lumibot.tools.polygon_helper.PolygonClient.create",
        MagicMock(return_value=MagicMock()),
    )
    yield


def _sample_frame(rows: int = 5) -> pl.DataFrame:
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    datetimes = [base + timedelta(minutes=i) for i in range(rows)]
    return pl.DataFrame(
        {
            "datetime": datetimes,
            "open": [100 + i for i in range(rows)],
            "high": [101 + i for i in range(rows)],
            "low": [99 + i for i in range(rows)],
            "close": [100.5 + i for i in range(rows)],
            "volume": [1_000 + i for i in range(rows)],
            "dividend": [0.0] * rows,
        }
    )


def test_polygon_polars_get_historical_prices(monkeypatch):
    monkeypatch.setattr(
        "lumibot.backtesting.polygon_backtesting_polars.polygon_helper_polars_optimized.get_price_data_from_polygon_polars",
        lambda *args, **kwargs: _sample_frame(10),
    )

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)
    backtester = PolygonDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key="demo",
        use_async=False,
        show_progress_bar=False,
    )

    asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    bars = backtester.get_historical_prices(asset, length=3, timestep="minute", quote=quote, return_polars=True)

    assert bars is not None
    assert isinstance(bars.polars_df, pl.DataFrame)
    assert bars.polars_df.height > 0
