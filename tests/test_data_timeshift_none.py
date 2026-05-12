from datetime import datetime, timedelta

import pandas as pd
import pytz

from lumibot.entities import Asset
from lumibot.entities.data import Data


def _minute_data() -> tuple[Data, list[datetime]]:
    tz = pytz.timezone("America/New_York")
    base_dt = tz.localize(datetime(2024, 1, 2, 9, 30))
    idx = [base_dt + timedelta(minutes=i) for i in range(5)]
    df = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "high": [101.0, 102.0, 103.0, 104.0, 105.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5],
            "volume": [100, 100, 100, 100, 100],
        },
        index=pd.DatetimeIndex(idx),
    )
    return Data(asset=Asset("SPY"), df=df, timestep="minute"), idx


def test_get_bars_accepts_timeshift_none():
    data, idx = _minute_data()
    dt = idx[-1]
    bars = data.get_bars(dt, length=2, timestep="minute", timeshift=None)
    assert isinstance(bars, pd.DataFrame)
    assert len(bars) == 2


def test_get_last_price_accepts_timeshift_none():
    data, idx = _minute_data()
    dt = idx[-1]
    assert data.get_last_price(dt, timeshift=None) == 104.0
