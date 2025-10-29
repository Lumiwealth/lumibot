"""
ThetaData pandas compatibility tests.

Ensures the helper returns pandas DataFrames by default and raises a clear
error when callers request polars output (which is intentionally unsupported
in this branch).
"""

from datetime import datetime, timezone
import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def _mock_cache_frame(start: datetime, rows: int = 8) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    df = pd.DataFrame(
        {
            "open": [200 + i for i in range(rows)],
            "high": [200.5 + i for i in range(rows)],
            "low": [199.5 + i for i in range(rows)],
            "close": [200.25 + i for i in range(rows)],
            "volume": [10_000 + 50 * i for i in range(rows)],
            "missing": [False] * rows,
        },
        index=index,
    )
    return df


def test_get_price_data_returns_pandas_when_cache_hit(monkeypatch, tmp_path):
    """Cache path with no missing intervals should return pandas DataFrame."""
    cache_file = tmp_path / "spy.minute.ohlc.parquet"
    cache_file.write_text("placeholder")

    mock_df = _mock_cache_frame(datetime(2025, 1, 1, tzinfo=timezone.utc))

    monkeypatch.setattr(
        thetadata_helper,
        "build_cache_filename",
        lambda *args, **kwargs: cache_file,
    )
    monkeypatch.setattr(thetadata_helper, "load_cache", lambda _: mock_df)
    monkeypatch.setattr(thetadata_helper, "get_missing_dates", lambda *args, **kwargs: [])
    monkeypatch.setattr(thetadata_helper, "update_cache", lambda *args, **kwargs: None)

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

    result = thetadata_helper.get_price_data(
        username="demo",
        password="demo",
        asset=asset,
        start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        timespan="minute",
        datastyle="ohlc",
        include_after_hours=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "missing" not in result.columns


def test_get_price_data_polars_request_rejected(monkeypatch, tmp_path):
    """Requesting return_polars=True should raise a clear ValueError."""
    cache_file = tmp_path / "spy.minute.ohlc.parquet"
    cache_file.write_text("placeholder")

    mock_df = _mock_cache_frame(datetime(2025, 1, 1, tzinfo=timezone.utc))

    monkeypatch.setattr(
        thetadata_helper,
        "build_cache_filename",
        lambda *args, **kwargs: cache_file,
    )
    monkeypatch.setattr(thetadata_helper, "load_cache", lambda _: mock_df)
    monkeypatch.setattr(thetadata_helper, "get_missing_dates", lambda *args, **kwargs: [])

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

    with pytest.raises(ValueError) as excinfo:
        thetadata_helper.get_price_data(
            username="demo",
            password="demo",
            asset=asset,
            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
            timespan="minute",
            datastyle="ohlc",
            include_after_hours=True,
            return_polars=True,
        )

    assert "polars output" in str(excinfo.value).lower()
