"""Cached aggregate timestamps must cover the provider's trading date."""

from datetime import date, datetime, timezone
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import polygon_helper as ph


START = datetime(2025, 1, 7, 5, tzinfo=timezone.utc)
END = datetime(2025, 1, 7, 21, tzinfo=timezone.utc)
DAY = date(2025, 1, 7)


@pytest.mark.parametrize("kind", ["stock", "option", "index"])
def test_prior_evening_stock_rows_do_not_cover_next_us_date(kind):
    asset = Asset("SYNTH", asset_type=kind, expiration=date(2025, 2, 21))
    frame = pd.DataFrame({"close": [100.0]}, index=pd.to_datetime(["2025-01-07T00:59:00Z"]))

    assert ph.get_missing_dates(frame, asset, START, END) == [DAY]


def test_confirmed_empty_us_date_still_counts_as_checked():
    frame = pd.DataFrame({"missing": [True]}, index=pd.to_datetime(["2025-01-07T00:00:00Z"]))

    assert ph.get_missing_dates(frame, Asset("SYNTH"), START, END) == []


def test_mixed_real_and_missing_rows_keep_their_own_date_conventions():
    frame = pd.DataFrame(
        {"close": [100.0, None], "missing": [False, True]},
        index=pd.to_datetime(["2025-01-07T00:59:00Z", "2025-01-08T00:00:00Z"]),
    )
    end = datetime(2025, 1, 8, 21, tzinfo=timezone.utc)

    assert ph.get_missing_dates(frame, Asset("SYNTH"), START, end) == [DAY]


def test_utc_crypto_cache_days_are_preserved():
    frame = pd.DataFrame({"close": [100.0]}, index=pd.to_datetime(["2025-01-07T00:59:00Z"]))

    assert ph.get_missing_dates(frame, Asset("SYNTH", asset_type="crypto"), START, END) == []


def test_naive_cache_dates_remain_compatible():
    frame = pd.DataFrame({"close": [100.0]}, index=pd.to_datetime(["2025-01-07T00:00:00"]))

    assert ph.get_missing_dates(frame, Asset("SYNTH"), START, END) == []


@pytest.mark.parametrize("available", [True, False])
def test_history_downloads_missing_us_session_despite_cached_prior_evening(monkeypatch, tmp_path, available):
    asset = Asset("SYNTH")
    monkeypatch.setattr(ph, "LUMIBOT_CACHE_FOLDER", tmp_path)
    monkeypatch.setattr(ph, "validate_cache", lambda forced, *args: forced)
    monkeypatch.setattr(ph, "tqdm", Mock())
    frame = pd.DataFrame(
        {"open": [99.0], "high": [100.0], "low": [98.0], "close": [100.0], "volume": [10]},
        index=pd.to_datetime(["2025-01-07T00:59:00Z"]),
    ).rename_axis("datetime")
    ph.update_cache(ph.build_cache_filename(asset, "minute"), frame)
    client = Mock()
    # Polygon aggregate response fields; a different session price exposes reuse.
    client.get_aggs.return_value = [{
        "t": int(datetime(2025, 1, 7, 14, 30, tzinfo=timezone.utc).timestamp() * 1000),
        "o": 120.0, "h": 122.0, "l": 119.0, "c": 121.0, "v": 500,
    }] if available else []
    monkeypatch.setattr(ph.PolygonClient, "create", Mock(return_value=client))

    result = ph.get_price_data_from_polygon("fixture", asset, START, END, "minute", max_workers=1)

    client.get_aggs.assert_called_once_with(
        ticker="SYNTH", from_=DAY, to=DAY, multiplier=1, timespan="minute", limit=50000,
    )
    if available:
        assert result.loc[pd.Timestamp("2025-01-07T14:30:00Z"), "open"] == 120.0
    else:
        assert result.empty
    client.get_aggs.reset_mock()
    repeated = ph.get_price_data_from_polygon("fixture", asset, START, END, "minute", max_workers=1)
    pd.testing.assert_frame_equal(repeated, result)
    client.get_aggs.assert_not_called()
