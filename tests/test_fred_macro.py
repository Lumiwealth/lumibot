from datetime import datetime, timezone

from lumibot.macro import FREDMacroData


class _Response:
    def __init__(self, *, payload=None, text=""):
        self._payload = payload
        self.text = text

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Strategy:
    def get_datetime(self):
        return datetime(2025, 1, 15, tzinfo=timezone.utc)


def test_fred_data_requires_api_key(monkeypatch, tmp_path):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    fred = FREDMacroData(_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    try:
        fred.get_series("DGS10")
    except ValueError as exc:
        assert "FRED_API_KEY is required" in str(exc)
    else:
        raise AssertionError("FRED macro data should require FRED_API_KEY")

    catalog = fred.list_series(category="rates")
    assert any(row["series_id"] == "DGS10" for row in catalog["series"])


def test_fred_api_mode_uses_vintage_params_and_filters_future_observations(monkeypatch, tmp_path):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(
            payload={
                "observations": [
                    {"date": "2024-12-01", "value": "4.1", "realtime_start": "2025-01-15", "realtime_end": "2025-01-15"},
                    {"date": "2025-01-15", "value": "4.3", "realtime_start": "2025-01-15", "realtime_end": "2025-01-15"},
                    {"date": "2025-01-16", "value": "4.4", "realtime_start": "2025-01-15", "realtime_end": "2025-01-15"},
                ]
            }
        )

    monkeypatch.setenv("FRED_API_KEY", "test-key")
    monkeypatch.setattr("lumibot.macro.fred.requests.get", fake_get)
    fred = FREDMacroData(_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    result = fred.get_series("DGS10", start="2024-01-01")
    assert result["source"] == "fred_api"
    assert result["point_in_time_safe"] is True
    assert result["uses_revised_data"] is False
    assert [row["date"] for row in result["observations"]] == ["2024-12-01", "2025-01-15"]

    params = calls[0][1]["params"]
    assert params["api_key"] == "test-key"
    assert params["realtime_start"] == "2025-01-15"
    assert params["realtime_end"] == "2025-01-15"
    assert params["observation_end"] == "2025-01-15"


def test_fred_snapshot_reports_per_series_errors(monkeypatch, tmp_path):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    fred = FREDMacroData(_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    result = fred.get_snapshot(["NOT_A_CURATED_SERIES"])
    assert result["values"] == {}
    assert "NOT_A_CURATED_SERIES" in result["errors"]
