"""
Tests for the `Data` entity pricing semantics.

Contract:
- `Data.get_last_price()` is trade/bar based only (open/close from bars).
- It must NEVER fall back to bid/ask midpoint (quote/mark pricing is accessed via `get_quote()`
  / `get_price_snapshot()`).
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
import pytz

from lumibot.entities import Asset
from lumibot.entities.data import Data


class TestDataGetLastPriceTradeOnly:
    def _create_data_with_prices(
        self,
        asset: Asset,
        close_prices,
        open_prices=None,
        bid_prices=None,
        ask_prices=None,
        timestep: str = "day",
    ) -> Data:
        if open_prices is None:
            open_prices = close_prices

        n = len(close_prices)
        tz = pytz.timezone("America/New_York")
        base_dt = tz.localize(datetime(2024, 1, 1, 9, 30))
        dates = [base_dt + timedelta(days=i) for i in range(n)]

        df_data = {
            "datetime": dates,
            "open": open_prices,
            "high": [
                max(o, c) if o is not None and c is not None else (o or c)
                for o, c in zip(open_prices, close_prices)
            ],
            "low": [
                min(o, c) if o is not None and c is not None else (o or c)
                for o, c in zip(open_prices, close_prices)
            ],
            "close": close_prices,
            "volume": [1000] * n,
        }

        if bid_prices is not None:
            df_data["bid"] = bid_prices
        if ask_prices is not None:
            df_data["ask"] = ask_prices

        df = pd.DataFrame(df_data).set_index("datetime")
        return Data(asset, df, timestep=timestep)

    def test_day_bars_returns_close(self):
        asset = Asset("SPY")
        close_prices = [100.0, 101.0, 102.0]
        data = self._create_data_with_prices(asset, close_prices)

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) == 102.0

    def test_intraday_returns_open_before_bar_completion(self):
        asset = Asset("SPY")
        tz = pytz.timezone("America/New_York")
        base_dt = tz.localize(datetime(2024, 1, 2, 9, 30))
        df = (
            pd.DataFrame(
                {
                    "datetime": [base_dt, base_dt + timedelta(minutes=1)],
                    "open": [100.0, 200.0],
                    "high": [110.0, 210.0],
                    "low": [90.0, 190.0],
                    "close": [110.0, 210.0],
                    "volume": [1000, 1000],
                }
            )
            .set_index("datetime")
        )

        data = Data(asset, df, timestep="minute")
        dt = base_dt + timedelta(minutes=1)
        assert data.get_last_price(dt) == 200.0

    def test_returns_none_when_close_missing_even_with_bid_ask(self):
        asset = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 1).date(),
            strike=400,
            right="CALL",
        )

        close_prices = [None, None, None]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]
        data = self._create_data_with_prices(
            asset,
            close_prices,
            open_prices=[None, None, None],
            bid_prices=bid_prices,
            ask_prices=ask_prices,
        )

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) is None

    def test_returns_none_when_close_nan_even_with_bid_ask(self):
        asset = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 1).date(),
            strike=400,
            right="CALL",
        )

        close_prices = [np.nan, np.nan, np.nan]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]
        data = self._create_data_with_prices(
            asset,
            close_prices,
            open_prices=[np.nan, np.nan, np.nan],
            bid_prices=bid_prices,
            ask_prices=ask_prices,
        )

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) is None

    def test_prefers_close_over_bid_ask(self):
        asset = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 1).date(),
            strike=400,
            right="CALL",
        )

        close_prices = [5.0, 5.0, 5.0]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]
        data = self._create_data_with_prices(
            asset,
            close_prices,
            bid_prices=bid_prices,
            ask_prices=ask_prices,
        )

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) == 5.0


def test_native_minute_bars_fast_returns_lazy_pandas_slice_until_used():
    asset = Asset("SPY")
    tz = pytz.timezone("America/New_York")
    idx = pd.date_range(tz.localize(datetime(2024, 1, 2, 9, 30)), periods=20, freq="1min")
    df = pd.DataFrame(
        {
            "open": range(20),
            "high": range(1, 21),
            "low": range(20),
            "close": range(2, 22),
            "volume": [100] * 20,
        },
        index=idx,
    )
    data = Data(asset, df, timestep="minute")

    bars_df = data.get_native_bars_fast(idx[10].to_pydatetime(), length=5, timestep="minute", mark_timezone=False)

    assert isinstance(bars_df, pd.DataFrame)
    assert len(bars_df) == 5
    assert getattr(bars_df, "_lumibot_real_df", None) is None
    assert bars_df["close"].iloc[-1] == 11
    assert getattr(bars_df, "_lumibot_real_df", None) is not None


def test_native_minute_bars_fast_lazy_slice_matches_common_pandas_ops():
    asset = Asset("SPY")
    tz = pytz.timezone("America/New_York")
    idx = pd.date_range(tz.localize(datetime(2024, 1, 2, 9, 30)), periods=20, freq="1min")
    df = pd.DataFrame(
        {
            "open": range(20),
            "high": range(1, 21),
            "low": range(20),
            "close": range(2, 22),
            "volume": [100] * 20,
        },
        index=idx,
    )
    data = Data(asset, df, timestep="minute")

    fast_df = data.get_native_bars_fast(idx[10].to_pydatetime(), length=5, timestep="minute", mark_timezone=False)
    slow_df = data.get_bars(idx[10].to_pydatetime(), length=5, timestep="minute", timeshift=None)

    ops = {
        "shape": lambda x: x.shape,
        "dtypes": lambda x: tuple(map(str, x.dtypes)),
        "iloc": lambda x: float(x.iloc[-1]["close"]),
        "tail": lambda x: float(x.tail(1)["close"].iloc[0]),
        "copy": lambda x: float(x.copy()["close"].iloc[-1]),
        "reset_index": lambda x: x.reset_index().shape,
        "to_numpy": lambda x: x.to_numpy().shape,
        "describe": lambda x: round(float(x.describe().loc["mean", "close"]), 4),
        "iterrows": lambda x: sum(1 for _ in x.iterrows()),
    }

    for op_name, op in ops.items():
        assert op(fast_df) == op(slow_df), op_name


def test_native_minute_bars_fast_lazy_slice_can_defer_return_column():
    asset = Asset("SPY")
    tz = pytz.timezone("America/New_York")
    idx = pd.date_range(tz.localize(datetime(2024, 1, 2, 9, 30)), periods=20, freq="1min")
    df = pd.DataFrame(
        {
            "open": range(20),
            "high": range(1, 21),
            "low": range(20),
            "close": range(2, 22),
            "volume": [100] * 20,
        },
        index=idx,
    )
    data = Data(asset, df, timestep="minute", assume_clean=True)
    data._defer_clean_returns = True
    data._initialize_clean_repaired_state()

    bars_df = data.get_native_bars_fast(idx[10].to_pydatetime(), length=5, timestep="minute", mark_timezone=False)

    assert getattr(bars_df, "_lumibot_real_df", None) is None
    assert "return" in bars_df.columns
    assert getattr(bars_df, "_lumibot_real_df", None) is None
    assert bars_df["return"].iloc[0] == pytest.approx(1 / 6)
    assert bars_df["return"].iloc[-1] == pytest.approx(0.1)


def test_native_day_bars_fast_cache_separates_timezone_marking():
    asset = Asset("SPY")
    tz = pytz.timezone("America/New_York")
    idx = pd.date_range(tz.localize(datetime(2024, 1, 1)), periods=6, freq="1D")
    df = pd.DataFrame(
        {
            "open": range(6),
            "high": range(1, 7),
            "low": range(6),
            "close": range(2, 8),
            "volume": [100] * 6,
        },
        index=idx,
    )
    data = Data(asset, df, timestep="day", assume_clean=True)
    query_dt = idx[4].to_pydatetime()

    unmarked = data.get_native_bars_fast(query_dt, length=2, timestep="day", mark_timezone=False)
    marked = data.get_native_bars_fast(query_dt, length=2, timestep="day", mark_timezone=True)

    assert not unmarked.attrs.get("_lumibot_skip_timezone")
    assert marked.attrs.get("_lumibot_skip_timezone") is True
    assert marked["close"].tolist() == unmarked["close"].tolist()


def test_assume_clean_data_initializes_legacy_maps_and_applies_date_filter():
    asset = Asset("SPY")
    tz = pytz.timezone("America/New_York")
    idx = pd.date_range(tz.localize(datetime(2024, 1, 1)), periods=8, freq="1D")
    df = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
            "high": [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5],
            "volume": [100] * 8,
        },
        index=idx,
    )

    data = Data(
        asset,
        df,
        date_start=idx[2].to_pydatetime(),
        date_end=idx[5].to_pydatetime(),
        timestep="day",
        assume_clean=True,
    )

    assert list(data.df.index) == list(idx[2:6])
    assert data.datalines["close"].dataline[-1] == 105.5
    assert data.iter_index_dict[idx[4].to_pydatetime()] == 2
    assert data.get_last_price(idx[4].to_pydatetime()) == 104.5


def test_fast_last_price_matches_native_bars_semantics():
    asset = Asset("SPY")
    tz = pytz.timezone("America/New_York")
    idx = pd.date_range(tz.localize(datetime(2024, 1, 2, 9, 30)), periods=12, freq="1min")
    close_values = [100.0 + i for i in range(12)]
    open_values = [99.5 + i for i in range(12)]
    open_values[10] = close_values[9]
    df = pd.DataFrame(
        {
            "open": open_values,
            "high": [value + 0.5 for value in close_values],
            "low": [value - 1.0 for value in close_values],
            "close": close_values,
            "volume": [100] * 12,
        },
        index=idx,
    )
    data = Data(asset, df, timestep="minute", assume_clean=True)

    query_dt = idx[10].to_pydatetime()
    bars = data.get_bars(query_dt, length=1, timestep="minute")

    assert data.get_last_price(query_dt) == close_values[9]
    assert data.get_last_price_fast(query_dt) == data.get_last_price(query_dt)
    assert data.get_last_price(query_dt) == bars["close"].iloc[-1]


def test_skip_clean_datalines_still_populates_legacy_accessors():
    asset = Asset("SPY")
    tz = pytz.timezone("America/New_York")
    idx = pd.date_range(tz.localize(datetime(2024, 1, 2, 9, 30)), periods=4, freq="1min")
    df = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [100.5, 101.5, 102.5, 103.5],
            "volume": [100] * 4,
        },
        index=idx,
    )
    data = Data.__new__(Data)
    data._skip_clean_datalines = True

    Data.__init__(data, asset, df, timestep="minute", assume_clean=True)

    assert data.datalines["datetime"].dataline[0] == idx[0]
    assert data.datalines["close"].dataline[-1] == 103.5
    assert data.get_last_price(idx[2].to_pydatetime()) == 102.0
