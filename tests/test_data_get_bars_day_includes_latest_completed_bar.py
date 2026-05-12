from __future__ import annotations

import datetime

import pandas as pd
import pytz

from lumibot.entities import Asset
from lumibot.entities.data import Data


def test_data_get_bars_day_includes_latest_completed_bar() -> None:
    """
    Regression test: day-cadence `get_bars()` should include the most recent completed bar.

    Bug (2026-01-04):
    - When requesting day bars at market open, Data.get_iter_count() returned the *asof* bar's index
      position, but slicing uses an exclusive end bound. This caused an extra off-by-one and made
      strategies lag by one full trading day (signals/trades shifted).
    """
    ny = pytz.timezone("America/New_York")
    idx = pd.DatetimeIndex(
        [
            ny.localize(datetime.datetime(2015, 8, 19, 16, 0)),
            ny.localize(datetime.datetime(2015, 8, 20, 16, 0)),
            ny.localize(datetime.datetime(2015, 8, 21, 16, 0)),
        ],
        name="datetime",
    )
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0],
            "high": [1.0, 2.0, 3.0],
            "low": [1.0, 2.0, 3.0],
            "close": [10.0, 20.0, 30.0],
            "volume": [0, 0, 0],
        },
        index=idx,
    )

    data = Data(asset=Asset("TQQQ", asset_type="stock"), df=df, timestep="day")

    # At the open on 2015-08-21, the latest *completed* day bar is 2015-08-20 16:00.
    bars = data.get_bars(ny.localize(datetime.datetime(2015, 8, 21, 9, 30)), length=2, timestep="day")
    assert bars.index[-1].date() == datetime.date(2015, 8, 20)
    assert float(bars["close"].iloc[-1]) == 20.0
