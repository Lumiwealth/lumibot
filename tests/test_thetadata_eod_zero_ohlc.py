from __future__ import annotations

from datetime import datetime

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_get_historical_eod_data_marks_all_zero_ohlc_rows_missing(monkeypatch):
    header_format = [
        "created",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ms_of_day",
        "ms_of_day2",
        "date",
    ]

    payload = {
        "header": {"format": header_format},
        "response": [
            ["2019-06-06T20:00:00Z", 100, 101, 99, 100, 1000000, 0, 0, "2019-06-06"],
            ["2019-06-07T20:00:00Z", 0, 0, 0, 0, 880, 0, 0, "2019-06-07"],
        ],
    }

    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_kwargs: payload)
    monkeypatch.setattr(thetadata_helper, "set_download_status", lambda *_a, **_k: None)
    monkeypatch.setattr(thetadata_helper, "advance_download_status_progress", lambda *_a, **_k: None)
    monkeypatch.setattr(thetadata_helper, "finalize_download_status", lambda *_a, **_k: None)

    asset = Asset("SPY", Asset.AssetType.STOCK)
    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=datetime(2019, 6, 6),
        end_dt=datetime(2019, 6, 7),
        apply_corporate_actions=False,
    )

    assert df is not None
    assert not df.empty
    assert "missing" in df.columns

    good_idx = pd.Timestamp("2019-06-06", tz="UTC")
    assert bool(df.loc[good_idx, "missing"]) is False
    assert df.loc[good_idx, "close"] == 100

    zero_idx = pd.Timestamp("2019-06-07", tz="UTC")
    assert bool(df.loc[zero_idx, "missing"]) is True
    assert pd.isna(df.loc[zero_idx, "open"])
    assert pd.isna(df.loc[zero_idx, "high"])
    assert pd.isna(df.loc[zero_idx, "low"])
    assert pd.isna(df.loc[zero_idx, "close"])
    assert df.loc[zero_idx, "volume"] == 0
