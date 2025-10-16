from datetime import datetime, timedelta, timezone
from typing import Callable, Tuple
import os

import pandas as pd
import polars as pl
import pytest

from lumibot.entities import Asset
from lumibot.backtesting.databento_backtesting_pandas import DataBentoDataBacktestingPandas
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_polars import ThetaDataBacktestingPolars


ProviderFactories = Tuple[
    Callable[[datetime, datetime], object],
    Callable[[datetime, datetime], object],
    bool,
]


def _sample_frame(rows: int = 6) -> pd.DataFrame:
    base = datetime(2025, 1, 2, 14, tzinfo=timezone.utc)
    datetimes = [base + timedelta(minutes=i) for i in range(rows)]
    frame = pd.DataFrame(
        {
            "datetime": datetimes,
            "open": [100 + i for i in range(rows)],
            "high": [101 + i for i in range(rows)],
            "low": [99 + i for i in range(rows)],
            "close": [100.5 + i for i in range(rows)],
            "volume": [1_000 + 10 * i for i in range(rows)],
        }
    )
    return frame


@pytest.mark.skip(reason="Parity smoke test disabled during polars migration cleanup.")
@pytest.mark.parametrize(
    "factories",
    [
        (
            lambda start, end: DataBentoDataBacktestingPandas(
                datetime_start=start,
                datetime_end=end,
                api_key="demo",
            ),
            lambda start, end: DataBentoDataBacktestingPolars(
                datetime_start=start,
                datetime_end=end,
                api_key="demo",
            ),
            False,
        ),
        (
            lambda start, end: ThetaDataBacktestingPandas(
                datetime_start=start,
                datetime_end=end,
                username=os.environ.get("THETADATA_USERNAME", "demo"),
                password=os.environ.get("THETADATA_PASSWORD", "demo"),
                show_progress_bar=False,
            ),
            lambda start, end: ThetaDataBacktestingPolars(
                datetime_start=start,
                datetime_end=end,
                username=os.environ.get("THETADATA_USERNAME", "demo"),
                password=os.environ.get("THETADATA_PASSWORD", "demo"),
                show_progress_bar=False,
            ),
            True,
        ),
    ],
)
def test_parse_source_symbol_bars_parity(factories: ProviderFactories):
    pandas_factory, polars_factory, include_quote = factories

    start = datetime(2025, 1, 2, tzinfo=timezone.utc)
    end = datetime(2025, 1, 3, tzinfo=timezone.utc)

    pandas_ds = pandas_factory(start, end)
    polars_ds = polars_factory(start, end)

    asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX) if include_quote else None

    sample_pd = _sample_frame()
    sample_pl = pl.from_pandas(sample_pd)

    pandas_bars = pandas_ds._parse_source_symbol_bars(sample_pd.copy(), asset, quote=quote)
    polars_bars = polars_ds._parse_source_symbol_bars(
        pl.from_pandas(sample_pd.copy()),
        asset,
        quote=quote,
        return_polars=True,
    )

    pandas_df = pandas_bars.pandas_df.reset_index(drop=False)
    polars_df = polars_bars.polars_df.to_pandas()

    # Normalize timezone information before comparison
    if "datetime" in pandas_df.columns and pandas_df["datetime"].dt.tz is not None:
        pandas_df["datetime"] = pandas_df["datetime"].dt.tz_convert("UTC")
    if "datetime" in polars_df.columns and polars_df["datetime"].dt.tz is not None:
        polars_df["datetime"] = polars_df["datetime"].dt.tz_convert("UTC")

    pd.testing.assert_frame_equal(
        pandas_df[["datetime", "open", "high", "low", "close", "volume"]],
        polars_df[["datetime", "open", "high", "low", "close", "volume"]],
        check_like=True,
    )
