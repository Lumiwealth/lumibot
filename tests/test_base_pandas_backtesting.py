# tests/test_base_pandas_backtesting.py

from datetime import datetime, timedelta
from collections import OrderedDict

import pytest
import pandas as pd
from lumibot.backtesting.base_pandas_backtesting import BasePandasBacktesting
from lumibot.data_sources import ExampleDataSource
from lumibot.entities import Asset
from lumibot.components.no_cache_system import NoCacheSystem


def test_initialization():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    max_memory = 1024

    data_source = BasePandasBacktesting(
        datetime_start=start_date,
        datetime_end=end_date,
        max_memory=max_memory,
        cache_system=NoCacheSystem(data_source=ExampleDataSource()),
    )

    assert data_source.datetime_start == start_date.replace(tzinfo=data_source.datetime_start.tzinfo)
    assert data_source.datetime_end == datetime(2023, 12, 30, 23, 59, tzinfo=data_source.datetime_end.tzinfo)
    assert data_source.MAX_STORAGE_BYTES == max_memory
    assert isinstance(data_source.pandas_data, dict)


def test_enforce_storage_limit(mocker):
    # Mock the logging.info to track eviction logs
    mock_logging = mocker.patch("lumibot.backtesting.base_pandas_backtesting.logging.info")

    # Set test parameters
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    max_memory = 1024
    data_source = BasePandasBacktesting(
        datetime_start=start_date,
        datetime_end=end_date,
        max_memory=max_memory,
        cache_system=NoCacheSystem(data_source=ExampleDataSource()),
    )

    # Mock data to emulate DataFrame with memory_usage() returning a pandas-like Series
    mock_data = mocker.MagicMock()
    # Fake the size of the stuff to be bigger than the max
    mock_data.df.memory_usage.return_value = mocker.MagicMock(sum=lambda: data_source.MAX_STORAGE_BYTES + 1)
    data_source.pandas_data = OrderedDict({"asset1": mock_data, "asset2": mock_data})

    # Call _enforce_storage_limit with the mocked data
    data_source._enforce_storage_limit()

    # Assert that all data was evicted due to memory constraints
    assert len(data_source.pandas_data) == 0
    # Assert that eviction logs were called twice (once per asset)
    assert mock_logging.call_count == 2


def test_update_data_calls_get_historical_prices_from_cache(mocker):
    mock_fetch = mocker.patch.object(NoCacheSystem, "get_historical_prices_from_cache", return_value=pd.DataFrame())

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    data_source = BasePandasBacktesting(
        datetime_start=start_date,
        datetime_end=end_date,
        cache_system=NoCacheSystem(data_source=ExampleDataSource()),
    )
    base_asset = Asset(symbol="AAPL", asset_type="stock")
    quote_asset = Asset(symbol="USD", asset_type="forex")
    timestep = "1D"

    data_source._update_pandas_data(
        asset=base_asset,
        quote=quote_asset,
        length=1,
        timestep=timestep
    )

    mock_fetch.assert_called_once_with(
        asset=base_asset,
        length=1,
        timestep='d',
        quote=quote_asset
    )


def test_get_start_datetime_and_ts_unit():
    length = 10
    timestep = "1D"
    start_buffer = timedelta(days=2)

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)

    data_source = BasePandasBacktesting(
        datetime_start=start_date,
        datetime_end=end_date,
        cache_system=NoCacheSystem(data_source=ExampleDataSource()),
    )

    current_dt = datetime(2023, 1, 11)
    start_dt, ts_unit = data_source.get_start_datetime_and_ts_unit(length, timestep, current_dt, start_buffer)

    assert start_dt == datetime(2023, 1, 11) - timedelta(days=12)
    assert ts_unit == "d"

