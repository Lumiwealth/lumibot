from datetime import datetime, timezone

from lumibot.macro import FXMacroData, MacroData


class _Response:
    def __init__(self, *, payload=None):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Strategy:
    def get_datetime(self):
        return datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)


def _payload():
    return {
        "data": [
            {
                "date": "2024-12-01",
                "val": "2.4",
                "announcement_datetime": "2025-01-15T11:30:00Z",
                "currency": "eur",
                "indicator": "inflation",
                "forecast": "2.5",
            },
            {
                "date": "2025-01-01",
                "val": "2.6",
                "announcement_datetime": "2025-01-15T12:30:00Z",
                "currency": "eur",
                "indicator": "inflation",
            },
        ]
    }


def test_fxmacrodata_uses_x_api_key_header_and_filters_future_rows(monkeypatch, tmp_path):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(payload=_payload())

    monkeypatch.setenv("FXMD_API_KEY", "test-fxmd-key")
    monkeypatch.setattr("lumibot.macro.fxmacrodata.requests.get", fake_get)
    fxmd = FXMacroData(_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    result = fxmd.get_series("eur", "inflation", start="2024-01-01")

    assert result["source"] == "fxmacrodata_api"
    assert result["point_in_time_safe"] is True
    assert [row["date"] for row in result["observations"]] == ["2024-12-01"]

    url, kwargs = calls[0]
    assert url == "https://api.fxmacrodata.com/v1/announcements/eur/inflation"
    assert kwargs["headers"] == {"X-API-Key": "test-fxmd-key"}
    assert "api_key" not in kwargs["params"]
    assert kwargs["params"]["start_date"] == "2024-01-01"
    assert kwargs["params"]["end_date"] == "2025-01-15"


def test_fxmacrodata_usd_requests_do_not_require_api_key(monkeypatch, tmp_path):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(payload={"data": [{"date": "2025-01-01", "val": "3.0"}]})

    monkeypatch.delenv("FXMD_API_KEY", raising=False)
    monkeypatch.delenv("FXMACRODATA_API_KEY", raising=False)
    monkeypatch.setattr("lumibot.macro.fxmacrodata.requests.get", fake_get)
    fxmd = FXMacroData(_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    result = fxmd.get_latest("usd", "inflation")

    assert result["latest"]["value"] == 3.0
    assert calls[0][1]["headers"] is None


def test_fxmacrodata_non_usd_requires_api_key(monkeypatch, tmp_path):
    monkeypatch.delenv("FXMD_API_KEY", raising=False)
    monkeypatch.delenv("FXMACRODATA_API_KEY", raising=False)
    fxmd = FXMacroData(_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    try:
        fxmd.get_series("jpy", "policy_rate")
    except ValueError as exc:
        assert "FXMD_API_KEY or FXMACRODATA_API_KEY is required" in str(exc)
    else:
        raise AssertionError("non-USD FXMacroData requests should require an API key")

    catalog = fxmd.list_indicators(category="rates")
    assert any(row["indicator"] == "policy_rate" for row in catalog["indicators"])


def test_fxmacrodata_snapshot_reports_per_indicator_errors(monkeypatch, tmp_path):
    def fake_get(url, **kwargs):
        if url.endswith("/policy_rate"):
            raise RuntimeError("upstream unavailable")
        return _Response(payload={"data": [{"date": "2025-01-01", "val": "3.0"}]})

    monkeypatch.setenv("FXMD_API_KEY", "test-fxmd-key")
    monkeypatch.setattr("lumibot.macro.fxmacrodata.requests.get", fake_get)
    fxmd = FXMacroData(_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    result = fxmd.get_snapshot("eur", ["inflation", "policy_rate"])

    assert result["values"]["inflation"]["value"] == 3.0
    assert "policy_rate" in result["errors"]


def test_macro_data_preserves_fred_methods_and_adds_fxmacrodata(tmp_path):
    macro = MacroData(
        _Strategy(),
        cache_dir=tmp_path / "fred",
        fxmacrodata_cache_dir=tmp_path / "fxmacrodata",
        min_request_interval_seconds=0,
    )

    assert macro.fred is macro
    assert macro.list_series(category="rates")["series"]
    assert macro.fxmacrodata.list_indicators(category="rates")["indicators"]
    assert macro.fxmd is macro.fxmacrodata
