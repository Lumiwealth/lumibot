import polars as pl
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars
from lumibot.entities import Asset

API_KEY = "test_key"


@pytest.fixture
def mocked_polars_helper(monkeypatch):
    monkeypatch.setattr(
        "lumibot.tools.databento_helper_polars.DataBentoClientPolars",
        MagicMock(),
    )


def _polars_frame(start_minute: int, rows: int = 5) -> pl.DataFrame:
    base = datetime(2025, 1, 6, 14, 0, tzinfo=timezone.utc)
    datetimes = [base + timedelta(minutes=start_minute + i) for i in range(rows)]
    return pl.DataFrame(
        {
            "datetime": datetimes,
            "open": [100.0 + i * 0.1 for i in range(rows)],
            "high": [100.2 + i * 0.1 for i in range(rows)],
            "low": [99.8 + i * 0.1 for i in range(rows)],
            "close": [100.1 + i * 0.1 for i in range(rows)],
            "volume": [1200 + i * 5 for i in range(rows)],
        }
    )


@pytest.mark.usefixtures("mocked_polars_helper")
def test_initialization_sets_properties():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 10, tzinfo=timezone.utc)
    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
    )

    assert backtester._api_key == API_KEY
    assert backtester.datetime_start == start
    # Implementation subtracts one minute from the end boundary to keep the last
    # candle fully formed.
    assert backtester.datetime_end == end - timedelta(minutes=1)


@pytest.mark.usefixtures("mocked_polars_helper")
@patch(
    "lumibot.backtesting.databento_backtesting_polars.databento_helper_polars.get_price_data_from_databento_polars"
)
def test_prefetch_data_populates_cache(mock_get_data):
    mock_get_data.return_value = _polars_frame(0, rows=8)
    start = datetime(2025, 2, 3, tzinfo=timezone.utc)
    end = datetime(2025, 2, 5, tzinfo=timezone.utc)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    if not hasattr(backtester, "prefetch_data"):
        pytest.skip("prefetch_data not implemented for polars backtesting backend")

    backtester.prefetch_data([asset], timestep="minute")
    assert (asset, Asset("USD", "forex")) in backtester._prefetched_assets
    mock_get_data.assert_called_once()


@pytest.mark.usefixtures("mocked_polars_helper")
@patch(
    "lumibot.backtesting.databento_backtesting_polars.databento_helper_polars.get_price_data_from_databento_polars"
)
def test_get_historical_prices_returns_bars(mock_get_data):
    mock_get_data.return_value = _polars_frame(0, rows=20)
    start = datetime(2025, 3, 3, tzinfo=timezone.utc)
    end = datetime(2025, 3, 4, tzinfo=timezone.utc)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    bars = backtester.get_historical_prices(asset, length=10, timestep="minute")
    assert bars is not None
    assert bars.polars_df.height == 10
    mock_get_data.assert_called()


@pytest.mark.usefixtures("mocked_polars_helper")
@patch(
    "lumibot.backtesting.databento_backtesting_polars.databento_helper_polars.get_price_data_from_databento_polars"
)
def test_get_last_price_returns_close(mock_get_data):
    mock_get_data.return_value = _polars_frame(0, rows=5)
    start = datetime(2025, 4, 1, tzinfo=timezone.utc)
    end = datetime(2025, 4, 2, tzinfo=timezone.utc)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    price = backtester.get_last_price(asset)
    expected = mock_get_data.return_value.tail(1)["close"][0]
    assert price == pytest.approx(float(expected))


@pytest.mark.usefixtures("mocked_polars_helper")
def test_get_historical_prices_non_future_returns_none():
    start = datetime(2025, 5, 1, tzinfo=timezone.utc)
    end = datetime(2025, 5, 2, tzinfo=timezone.utc)
    asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    bars = backtester.get_historical_prices(asset, length=5)
    assert bars is None
