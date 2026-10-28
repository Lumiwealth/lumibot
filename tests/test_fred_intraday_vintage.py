"""Daily vintage dates must not imply that a morning knew that day's close."""
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from lumibot.macro import FREDMacroData


@pytest.mark.parametrize("as_of,expected", [(None, "2025-01-14"),
    ("2025-01-15", "2025-01-14"), ("2025-01-20", "2025-01-14"),
    ("2025-01-10", "2025-01-10")])
def test_intraday_backtest_uses_completed_vintage_and_excludes_same_day_close(monkeypatch, tmp_path, as_of, expected):
    strategy = SimpleNamespace(is_backtesting=True,
        get_datetime=lambda: datetime(2025, 1, 15, 9, 30, tzinfo=ZoneInfo("America/New_York")))
    fred = FREDMacroData(strategy, api_key="synthetic", cache_dir=tmp_path)
    calls = []

    def observations(url, params, cache_path):
        calls.append(params)
        return {"observations": [
            {"date": "2025-01-10", "value": "18.0"},
            {"date": "2025-01-14", "value": "19.0"},
            {"date": "2025-01-15", "value": "31.0"},
        ]}

    monkeypatch.setattr(fred, "_get_json", observations)
    result = fred.get_snapshot(["VIXCLS"], as_of=as_of)
    assert result["errors"] == {}
    assert result["as_of"] == expected
    assert result["values"]["VIXCLS"]["date"] <= expected
    assert calls[0]["realtime_start"] == calls[0]["realtime_end"] == expected
    assert calls[0]["observation_end"] == expected
    assert result["temporal_policy"] == "previous_completed_fred_vintage"
    assert result["intraday_release_times_verified"] is False


def test_completed_vintage_uses_fred_timezone_and_live_remains_current(monkeypatch, tmp_path):
    now = datetime(2025, 1, 16, 0, 30, tzinfo=ZoneInfo("UTC"))
    strategy = SimpleNamespace(is_backtesting=True, get_datetime=lambda: now)
    fred = FREDMacroData(strategy, api_key="synthetic", cache_dir=tmp_path)
    monkeypatch.setattr(fred, "_get_json", lambda *a: {"observations": []})
    assert fred.get_snapshot(["VIXCLS"])["as_of"] == "2025-01-14"
    strategy.is_backtesting = False
    assert fred.get_snapshot(["VIXCLS"], as_of="2025-01-15")["as_of"] == "2025-01-15"
