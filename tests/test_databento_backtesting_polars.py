import pandas as pd
import polars as pl
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from lumibot.tools.databento_helper_polars import DataBentoAuthenticationError
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars
from lumibot.entities import Asset

API_KEY = "test_key"


@pytest.fixture
def mocked_polars_helper(monkeypatch):
    monkeypatch.setattr(
        "lumibot.tools.databento_helper_polars.DataBentoClient",
        MagicMock(),
    )
    monkeypatch.setattr(
        "lumibot.tools.databento_helper_polars.DATABENTO_AVAILABLE",
        True,
    )
    monkeypatch.setattr(
        "lumibot.tools.databento_helper_polars._fetch_and_update_futures_multiplier",
        lambda *args, **kwargs: None,
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


@pytest.mark.usefixtures("mocked_polars_helper")
def test_get_historical_prices_reuses_cache(monkeypatch, tmp_path):
    """Second identical request should hit disk cache instead of refetching."""

    cache_dir = tmp_path / "databento_cache"
    cache_dir.mkdir()

    monkeypatch.setattr(
        "lumibot.tools.databento_helper_polars.LUMIBOT_DATABENTO_CACHE_FOLDER",
        str(cache_dir),
        raising=False,
    )
    monkeypatch.setattr(
        "lumibot.backtesting.databento_backtesting_polars.databento_helper.LUMIBOT_DATABENTO_CACHE_FOLDER",
        str(cache_dir),
        raising=False,
    )

    fetch_calls = 0

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_historical_data(self, dataset, symbols, schema, start, end, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            index = pd.date_range(start=start, periods=5, freq="1min", tz="UTC")
            return pd.DataFrame(
                {
                    "ts_event": index,
                    "open": [100.0 + i for i in range(5)],
                    "high": [100.5 + i for i in range(5)],
                    "low": [99.5 + i for i in range(5)],
                    "close": [100.2 + i for i in range(5)],
                    "volume": [1_000 + 10 * i for i in range(5)],
                }
            )

    monkeypatch.setattr(
        "lumibot.tools.databento_helper_polars.DataBentoClient",
        FakeClient,
    )

    start = datetime(2025, 7, 1, tzinfo=timezone.utc)
    end = datetime(2025, 7, 2, tzinfo=timezone.utc)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    first_bars = backtester.get_historical_prices(asset, length=5, timestep="minute", return_polars=True)
    second_bars = backtester.get_historical_prices(asset, length=5, timestep="minute", return_polars=True)

    assert first_bars is not None and second_bars is not None
    pd.testing.assert_frame_equal(second_bars.pandas_df, first_bars.pandas_df)
    assert fetch_calls == 1, "Expected cached response on second call"
    assert list(cache_dir.glob("*.parquet")), "Cache directory should contain parquet artifacts"


@pytest.mark.usefixtures("mocked_polars_helper")
def test_auth_failure_propagates(monkeypatch):
    start = datetime(2025, 1, 6, tzinfo=timezone.utc)
    end = datetime(2025, 1, 7, tzinfo=timezone.utc)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    def boom(*args, **kwargs):
        raise DataBentoAuthenticationError("401 auth_authentication_failed")

    monkeypatch.setattr(
        "lumibot.backtesting.databento_backtesting_polars.databento_helper.get_price_data_from_databento",
        boom,
    )
    monkeypatch.setattr(
        "lumibot.tools.databento_helper_polars.get_price_data_from_databento",
        boom,
    )

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    with pytest.raises(DataBentoAuthenticationError):
        backtester.get_historical_prices(asset, length=1, timestep="minute", return_polars=True)

@patch(
    "lumibot.backtesting.databento_backtesting_polars.databento_helper.get_price_data_from_databento"
)
@pytest.mark.usefixtures("mocked_polars_helper")
def test_polars_no_future_minutes(mock_get_data, mocked_polars_helper):
    base = datetime(2025, 1, 6, 14, 30, tzinfo=timezone.utc)
    frame = pl.DataFrame(
        {
            "datetime": [base - timedelta(minutes=1), base + timedelta(minutes=1)],
            "open": [4300.0, 4302.0],
            "high": [4300.5, 4302.5],
            "low": [4299.5, 4301.5],
            "close": [4300.2, 4302.2],
            "volume": [1500, 1510],
        }
    )
    mock_get_data.return_value = frame

    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
    backtester = DataBentoDataBacktestingPolars(
        datetime_start=base - timedelta(days=1),
        datetime_end=base + timedelta(days=1),
        api_key=API_KEY,
        show_progress_bar=False,
    )
    backtester._datetime = base

    bars = backtester.get_historical_prices(
        asset,
        length=1,
        timestep="minute",
        return_polars=True,
    )

    assert bars is not None
    # Ensure we never look past the current iteration timestamp.
    assert bars.polars_df["datetime"][-1] <= base


@pytest.mark.usefixtures("mocked_polars_helper")
@patch(
    "lumibot.backtesting.databento_backtesting_polars.databento_helper.get_price_data_from_databento"
)
@pytest.mark.parametrize(
    "timeshift,expected_offsets",
    [
        (0, [9, 10, 11]),
        (-2, [11, 12, 13]),
        (2, [7, 8, 9]),
    ],
)
def test_pull_source_bars_uses_index_search(mock_get_data, timeshift, expected_offsets, monkeypatch):
    """Ensure the optimized slicing path returns the same bars for various timeshifts."""
    mock_get_data.return_value = _polars_frame(0, rows=20)
    start = datetime(2025, 1, 6, tzinfo=timezone.utc)
    end = datetime(2025, 1, 7, tzinfo=timezone.utc)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    backtester = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=API_KEY,
        show_progress_bar=False,
    )

    current_dt = datetime(2025, 1, 6, 14, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(backtester, "get_datetime", lambda: current_dt)

    result = backtester._pull_source_symbol_bars(
        asset,
        length=3,
        timestep="minute",
        timeshift=timeshift,
    )

    assert result is not None
    result_offsets = []
    for dt in result["datetime"].to_list():
        minutes_delta = int((dt - datetime(2025, 1, 6, 14, 0, tzinfo=timezone.utc)).total_seconds() // 60)
        result_offsets.append(minutes_delta)

    assert result_offsets == expected_offsets
