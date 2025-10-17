"""Parity checks between DataBento pandas and polars backends."""

import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytz

from lumibot.backtesting.databento_backtesting_pandas import DataBentoDataBacktestingPandas as DataBentoPandas
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars as DataBentoPolars
from lumibot.entities import Asset
from lumibot.credentials import DATABENTO_CONFIG
from lumibot.tools import databento_helper, databento_helper_polars

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


def _clear_databento_caches():
    for cache_dir in (
        databento_helper.LUMIBOT_DATABENTO_CACHE_FOLDER,
        databento_helper_polars.LUMIBOT_DATABENTO_CACHE_FOLDER,
    ):
        path = Path(cache_dir)
        if path.exists():
            shutil.rmtree(path)


def test_databento_source_identifier():
    """Verify polars has correct SOURCE identifier for broker timeshift routing.

    Regression test for issue where polars inherited SOURCE='POLARS' instead of
    'DATABENTO_POLARS', preventing broker from applying -2 minute timeshift.

    This SOURCE identifier determines how the broker applies timeshift during order fills:
    - PANDAS → uses SOURCE=="PANDAS" branch (line 1442 in backtesting_broker.py)
    - DATABENTO_POLARS → uses SOURCE=="DATABENTO_POLARS" branch (line 1503), gets timeshift=-2

    Without the correct SOURCE, polars would never receive timeshift=-2, causing ~14% portfolio divergence.
    """
    pandas_ds = DataBentoPandas(
        datetime_start=datetime(2024, 1, 1),
        datetime_end=datetime(2024, 1, 2),
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )
    polars_ds = DataBentoPolars(
        datetime_start=datetime(2024, 1, 1),
        datetime_end=datetime(2024, 1, 2),
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Both must have correct SOURCE identifiers for broker routing
    # Pandas inherits SOURCE="PANDAS" from PandasData parent class
    # Polars must override to SOURCE="DATABENTO_POLARS" for broker timeshift routing
    assert pandas_ds.SOURCE == "PANDAS", f"Pandas SOURCE is {pandas_ds.SOURCE}, expected PANDAS"
    assert polars_ds.SOURCE == "DATABENTO_POLARS", f"Polars SOURCE is {polars_ds.SOURCE}, expected DATABENTO_POLARS"


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_databento_price_parity():
    """Ensure pandas and polars backends deliver identical prices."""

    _clear_databento_caches()

    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 9, 15, 0, 0))
    end = tz.localize(datetime(2025, 9, 29, 23, 59))
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    pandas_ds = DataBentoPandas(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )
    polars_ds = DataBentoPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Prime caches
    pandas_bars = pandas_ds.get_historical_prices(asset, 500, timestep="minute").df.sort_index()
    polars_bars = polars_ds.get_historical_prices(asset, 500, timestep="minute").df.sort_index()

    candidate_columns = ["open", "high", "low", "close", "volume", "vwap"]
    common_columns = [col for col in candidate_columns if col in pandas_bars.columns and col in polars_bars.columns]
    assert common_columns, "No shared OHLCV columns between pandas and polars DataFrames"

    aligned_pandas = pandas_bars[common_columns].copy()
    aligned_polars = polars_bars[common_columns].copy()

    for col in common_columns:
        dtype_left = aligned_pandas[col].dtype
        dtype_right = aligned_polars[col].dtype
        if dtype_left != dtype_right:
            target_dtype = np.promote_types(dtype_left, dtype_right)
            aligned_pandas[col] = aligned_pandas[col].astype(target_dtype)
            aligned_polars[col] = aligned_polars[col].astype(target_dtype)

    pd.testing.assert_frame_equal(
        aligned_pandas,
        aligned_polars,
        check_exact=True,
        check_index_type=True,
        check_column_type=True,
    )

    checkpoints = [
        (0, 0),
        (3, 40),
        (4, 0),
        (7, 35),
        (11, 5),
        (14, 5),
    ]

    for hour, minute in checkpoints:
        current_dt = tz.localize(datetime(2025, 9, 15, hour, minute))
        pandas_ds._datetime = current_dt
        polars_ds._datetime = current_dt
        pandas_price = pandas_ds.get_last_price(asset)
        polars_price = polars_ds.get_last_price(asset)
        assert pandas_price == polars_price, (
            f"Mismatch at {current_dt}: pandas={pandas_price}, polars={polars_price}"
        )
