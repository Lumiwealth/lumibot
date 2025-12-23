import numpy as np
import pandas as pd

from lumibot.entities import Asset
from lumibot.entities.data import Data


def test_repair_times_and_fill_keeps_daily_quotes_ffilled_across_gaps():
    """Daily option NBBO (EOD bid/ask) must stay available at the next session's tick.

    In daily backtests, the simulation timestamps are typically at market open (or pre-open),
    while the daily bars/quotes are timestamped at market close. We intentionally forward-fill
    the prior session's EOD NBBO so option mark-to-market stays stable between observations.
    """

    asset = Asset("TEST", asset_type=Asset.AssetType.OPTION)

    df = pd.DataFrame(
        {
            "close": [0.0, 0.0, 0.0],
            "bid": [100.0, 110.0, 120.0],
            "ask": [102.0, 112.0, 122.0],
            "volume": [0, 0, 0],
        },
        index=pd.DatetimeIndex(
            [
                "2024-09-19 21:00:00+00:00",
                "2024-09-20 21:00:00+00:00",
                "2024-09-23 21:00:00+00:00",
            ]
        ),
    )

    data = Data(asset, df, timestep="day")

    idx = pd.DatetimeIndex(
        [
            "2024-09-20 13:30:00+00:00",
            "2024-09-23 13:30:00+00:00",
        ]
    )
    data.repair_times_and_fill(idx)

    assert data.df.loc[idx[0], "bid"] == 100.0
    assert data.df.loc[idx[0], "ask"] == 102.0

    assert not np.isnan(data.df.loc[idx[1], "bid"])
    assert not np.isnan(data.df.loc[idx[1], "ask"])
    assert data.df.loc[idx[1], "bid"] == 110.0
    assert data.df.loc[idx[1], "ask"] == 112.0

