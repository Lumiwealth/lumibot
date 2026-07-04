import datetime as dt

import pandas as pd

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
