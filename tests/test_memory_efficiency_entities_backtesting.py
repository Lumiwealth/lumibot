import datetime as dt

import pandas as pd

from lumibot.backtesting.alpaca_backtesting import AlpacaBacktesting
from lumibot.entities import Asset
from lumibot.entities.data import Data


def _ohlcv_frame(index: pd.DatetimeIndex) -> pd.DataFrame:
    row_count = len(index)
    return pd.DataFrame(
        {
            "open": range(row_count),
            "high": range(row_count),
            "low": range(row_count),
            "close": range(row_count),
            "volume": 1,
        },
        index=index,
    )


def test_large_data_repair_avoids_retained_iter_index_dict_and_preserves_lookup():
    index = pd.date_range("2024-01-01", periods=50_001, freq="min", tz="America/New_York")
    data = Data(Asset("MEM"), _ohlcv_frame(index), timestep="minute")

    data.repair_times_and_fill(index)

    assert data.iter_index_dict == {}
    assert data.get_iter_count(index[-1].to_pydatetime()) == len(index) - 1
    assert data.get_iter_count(index[123].to_pydatetime() + dt.timedelta(seconds=30)) == 123

    lazy_iter_index = data.iter_index
    assert lazy_iter_index.loc[index[123]] == 123


def test_alpaca_reindex_and_fill_batches_missing_daily_rows():
    trading_times = pd.date_range("2024-01-01", periods=6, freq="D", tz="America/New_York")
    existing = trading_times[[0, 3, 5]]
    df = pd.DataFrame(
        {
            "timestamp": existing,
            "open": [1.0, 4.0, 6.0],
            "high": [1.0, 4.0, 6.0],
            "low": [1.0, 4.0, 6.0],
            "close": [1.0, 4.0, 6.0],
            "volume": [10.0, 40.0, 60.0],
        }
    )
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source._normalize_timestep_for_source = lambda timestep: (timestep, None)

    result = data_source._reindex_and_fill(df=df, trading_times=trading_times, timestep="day")

    assert result["timestamp"].tolist() == list(trading_times)
    assert result["close"].tolist() == [1.0, 1.0, 1.0, 4.0, 4.0, 6.0]
    assert result["volume"].tolist() == [10.0, 0.0, 0.0, 40.0, 0.0, 60.0]
