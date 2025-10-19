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
        "lumibot.tools.databento_helper_polars.DataBentoClient",
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
    "lumibot.backtesting.databento_backtesting_polars.databento_helper.get_price_data_from_databento"
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
    "lumibot.backtesting.databento_backtesting_polars.databento_helper.get_price_data_from_databento"
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
    "lumibot.backtesting.databento_backtesting_polars.databento_helper.get_price_data_from_databento"
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


@pytest.mark.usefixtures("mocked_polars_helper")
def test_databento_polars_quote_midpoint(monkeypatch):
    start = datetime(2025, 6, 1, tzinfo=timezone.utc)
    end = datetime(2025, 6, 2, tzinfo=timezone.utc)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    current_dt = datetime(2025, 6, 1, 15, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(backtester, "get_datetime", lambda: current_dt)

    sample_df = pl.DataFrame(
        {
            "datetime": [current_dt],
            "open": [4300.0],
            "high": [4301.0],
            "low": [4299.5],
            "close": [4300.5],
            "volume": [1500],
            "bid": [4299.75],
            "ask": [4301.25],
            "bid_size": [5],
            "ask_size": [6],
        }
    )

    def fake_pull(self, *_args, **_kwargs):
        return sample_df

    monkeypatch.setattr(backtester, "_pull_source_symbol_bars", fake_pull.__get__(backtester, type(backtester)))

    quote = backtester.get_quote(asset)
    expected_mid = (sample_df["bid"][0] + sample_df["ask"][0]) / 2.0

    assert quote.mid_price == pytest.approx(expected_mid)
    assert quote.price == pytest.approx(expected_mid)
    assert getattr(quote, "source", None) == "polars"

