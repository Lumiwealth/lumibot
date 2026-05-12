from datetime import datetime

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_advance_download_status_progress_is_contract_specific_for_options():
    thetadata_helper.clear_download_status()

    active_contract = Asset(
        symbol="SPX",
        asset_type="option",
        strike=3000,
        expiration=datetime(2026, 12, 5).date(),
        right="CALL",
    )
    other_contract = Asset(
        symbol="SPX",
        asset_type="option",
        strike=3100,
        expiration=datetime(2026, 12, 5).date(),
        right="CALL",
    )

    thetadata_helper.set_download_status(
        active_contract,
        "USD",
        "quote",
        "day",
        0,
        12,
    )

    # Same underlying symbol, different strike -> must not advance.
    thetadata_helper.advance_download_status_progress(asset=other_contract, data_type="quote", timespan="day", step=1)
    status = thetadata_helper.get_download_status()
    assert status["current"] == 0

    # Matching contract -> advances.
    thetadata_helper.advance_download_status_progress(asset=active_contract, data_type="quote", timespan="day", step=1)
    status = thetadata_helper.get_download_status()
    assert status["current"] == 1


def test_get_historical_eod_data_tracks_progress_per_outer_window(monkeypatch):
    thetadata_helper.clear_download_status()

    asset = Asset(symbol="AAPL", asset_type="stock")

    # Choose a range that deterministically creates 2 windows with max_span=364 days.
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime(2025, 1, 10)

    calls = {"count": 0}

    def fake_get_request(*_args, **_kwargs):
        calls["count"] += 1
        return {
            "header": {"format": ["date", "ms_of_day", "ms_of_day2", "open", "high", "low", "close", "volume"]},
            "response": [["2024-01-02", 0, 0, 1.0, 1.0, 1.0, 1.0, 100]],
        }

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)

    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=start_dt,
        end_dt=end_dt,
        datastyle="ohlc",
        apply_corporate_actions=False,
    )

    assert df is not None
    status = thetadata_helper.get_download_status()
    assert status["active"] is False
    assert status["timespan"] == "day"
    assert status["total"] == 2
    assert status["current"] == 2
    assert status["progress"] == 100
