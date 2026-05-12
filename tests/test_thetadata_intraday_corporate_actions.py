from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_get_price_data_intraday_is_split_adjusted_in_backtests(monkeypatch, tmp_path) -> None:
    """
    Regression test (NVDA 2022 backtests):

    - Option-chain strike lists are normalized using split-adjusted DAILY reference prices.
    - If intraday stock OHLC is left unadjusted, strategies can see a 10x scale mismatch after a split
      (e.g., underlying ~300 vs strikes ~30 for NVDA after the 2024-06-10 10-for-1 split).

    This test ensures that, during backtests, intraday pricing returned by get_price_data is also
    corporate-action adjusted to the same scale as day bars / chain strike normalization.
    """

    monkeypatch.setenv("IS_BACKTESTING", "true")
    monkeypatch.delenv("THETADATA_APPLY_CORPORATE_ACTIONS_INTRADAY", raising=False)

    cache_file = tmp_path / "nvda.minute.ohlc.parquet"
    cache_file.write_text("placeholder")

    idx = pd.date_range(start=datetime(2022, 1, 3, tzinfo=UTC), periods=3, freq="1min", tz="UTC")
    raw = pd.DataFrame(
        {
            "open": [300.0, 301.0, 302.0],
            "high": [300.5, 301.5, 302.5],
            "low": [299.5, 300.5, 301.5],
            "close": [300.25, 301.25, 302.25],
            "volume": [1_000, 1_100, 1_200],
            "missing": [False, False, False],
        },
        index=idx,
    )

    class DisabledCacheManager:
        enabled = False
        mode = None

        def ensure_local_file(self, *args, **kwargs):
            return False

        def on_local_update(self, *args, **kwargs):
            return False

    monkeypatch.setattr(thetadata_helper, "get_backtest_cache", lambda: DisabledCacheManager())
    monkeypatch.setattr(thetadata_helper, "build_cache_filename", lambda *args, **kwargs: cache_file)
    monkeypatch.setattr(thetadata_helper, "load_cache", lambda *_a, **_k: raw)
    monkeypatch.setattr(thetadata_helper, "get_missing_dates", lambda *args, **kwargs: [])
    monkeypatch.setattr(thetadata_helper, "update_cache", lambda *args, **kwargs: None)

    # No dividends needed for this test.
    monkeypatch.setattr(
        thetadata_helper,
        "_get_theta_dividends",
        lambda *_a, **_k: pd.DataFrame(columns=["event_date", "cash_amount"]),
    )

    # NVDA 10-for-1 split effective 2024-06-10 (split-adjusted trading begins 2024-06-10).
    monkeypatch.setattr(
        thetadata_helper,
        "_get_theta_splits",
        lambda *_a, **_k: pd.DataFrame(
            {
                "event_date": [pd.Timestamp("2024-06-10 00:00:00+00:00")],
                "ratio": [10.0],
            }
        ),
    )

    asset = Asset("NVDA", asset_type=Asset.AssetType.STOCK)
    result = thetadata_helper.get_price_data(
        username="demo",
        password="demo",
        asset=asset,
        start=idx.min().to_pydatetime(),
        end=idx.max().to_pydatetime(),
        timespan="minute",
        datastyle="ohlc",
        include_after_hours=True,
    )

    assert result is not None
    assert not result.empty

    # Split-adjusted prices should be divided by 10 for dates before 2024-06-10.
    assert result["open"].tolist() == [30.0, 30.1, 30.2]
    assert result["close"].tolist() == [30.025, 30.125, 30.225]
    assert result["_split_adjusted"].all()
