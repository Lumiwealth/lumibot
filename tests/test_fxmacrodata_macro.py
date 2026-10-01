"""Tests for FXMacroData macro data support."""

# pylint: disable=missing-class-docstring,missing-function-docstring

from datetime import datetime, timezone

import pytest

from lumibot.macro import FXMacroData, MacroData


class _Response:
    def __init__(self, *, payload=None, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Strategy:
    def get_datetime(self):
        return datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)


class _BacktestingStrategy(_Strategy):
    is_backtesting = True


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


@pytest.fixture
def fxmacrodata_factory(monkeypatch, tmp_path):
    """Create FXMacroData instances with isolated environment and network state."""

    def _build(strategy, fake_get, *, api_key=None):
        monkeypatch.delenv("FXMD_API_KEY", raising=False)
        monkeypatch.delenv("FXMACRODATA_API_KEY", raising=False)
        if api_key is not None:
            monkeypatch.setenv("FXMD_API_KEY", api_key)
        monkeypatch.setattr("lumibot.macro.fxmacrodata.requests.get", fake_get)
        return FXMacroData(strategy, cache_dir=tmp_path, min_request_interval_seconds=0)

    return _build


def test_fxmacrodata_uses_x_api_key_header_and_filters_future_rows(fxmacrodata_factory):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(payload=_payload())

    fxmd = fxmacrodata_factory(_Strategy(), fake_get, api_key="test-fxmd-key")

    result = fxmd.get_series("eur", "inflation", start="2024-01-01")

    assert result["source"] == "fxmacrodata_api"
    assert "point_in_time_safe" not in result
    assert [row["date"] for row in result["observations"]] == ["2024-12-01"]
    assert result["publication_time"] == {
        "rows_with_announcement_datetime": 1,
        "rows_without_announcement_datetime": 0,
        "rows_dropped_undated": 0,
        "rows_dropped_without_announcement_datetime": 0,
        "approximate": False,
        "publication_time_status_counts": {"not_reported": 1},
    }
    assert result["observations"][0]["gated_on"] == "announcement_datetime"

    url, kwargs = calls[0]
    assert url == "https://api.fxmacrodata.com/v1/announcements/eur/inflation"
    assert kwargs["headers"] == {"X-API-Key": "test-fxmd-key"}
    assert "api_key" not in kwargs["params"]
    assert kwargs["params"]["start_date"] == "2024-01-01"
    assert kwargs["params"]["end_date"] == "2025-01-15"


def test_fxmacrodata_drops_rows_without_parseable_dates(fxmacrodata_factory):
    def fake_get(url, **kwargs):
        return _Response(payload={"data": [{"val": "9.9"}, {"date": "2025-01-01", "val": "3.0"}]})

    fxmd = fxmacrodata_factory(_Strategy(), fake_get)

    result = fxmd.get_series("usd", "inflation")

    assert [row["date"] for row in result["observations"]] == ["2025-01-01"]
    assert result["observations"][0]["value"] == 3.0
    assert result["publication_time"]["rows_dropped_undated"] == 1
    assert result["publication_time"]["rows_without_announcement_datetime"] == 1
    # Outside a backtest the period-date fallback is kept but marked approximate.
    assert result["observations"][0]["gated_on"] == "period_date"
    assert result["publication_time"]["approximate"] is True


def test_fxmacrodata_backtests_exclude_rows_without_announcement_datetime(
    fxmacrodata_factory,
):
    def fake_get(url, **kwargs):
        return _Response(
            payload={
                "data": [
                    # Period 2025-01-01, release time unknown: in a backtest this
                    # could be seen before it was actually published.
                    {"date": "2025-01-01", "val": "3.0"},
                    {
                        "date": "2024-12-01",
                        "val": "2.7",
                        "announcement_datetime": "2025-01-10T13:30:00Z",
                    },
                ]
            }
        )

    fxmd = fxmacrodata_factory(_BacktestingStrategy(), fake_get)

    series = fxmd.get_series("usd", "inflation")
    latest = fxmd.get_latest("usd", "inflation")

    assert [row["date"] for row in series["observations"]] == ["2024-12-01"]
    assert series["observations"][0]["gated_on"] == "announcement_datetime"
    assert series["publication_time"]["rows_dropped_without_announcement_datetime"] == 1
    assert series["publication_time"]["approximate"] is False
    assert latest["latest"]["value"] == 2.7


def test_fxmacrodata_get_latest_pages_past_unpublished_rows(fxmacrodata_factory):
    calls = []
    # 25 rows whose period is before the strategy date but whose release is
    # still in the future, then the newest row actually published by as_of.
    unpublished = [
        {
            "date": "2025-01-14",
            "val": str(index),
            "announcement_datetime": "2025-01-20T13:30:00Z",
        }
        for index in range(25)
    ]
    published = {
        "date": "2024-12-01",
        "val": "2.7",
        "announcement_datetime": "2025-01-10T13:30:00Z",
    }
    older = {
        "date": "2024-11-01",
        "val": "2.6",
        "announcement_datetime": "2024-12-10T13:30:00Z",
    }
    rows = [*unpublished, published, older]
    fxmd = fxmacrodata_factory(_Strategy(), _paged_fake_get(rows, calls))

    result = fxmd.get_latest("usd", "inflation")

    assert result["latest"]["value"] == 2.7
    assert result["latest"]["date"] == "2024-12-01"
    assert [row["value"] for row in result["observations"]] == [2.6, 2.7]
    # First page keeps the old 20-row request, later pages use the API maximum.
    assert [call["limit"] for call in calls] == [20, 100]
    assert [call.get("offset") for call in calls] == [None, 20]


def test_fxmacrodata_get_latest_stops_after_first_eligible_page(fxmacrodata_factory):
    calls = []
    rows = _monthly_rows(250)
    fxmd = fxmacrodata_factory(_Strategy(), _paged_fake_get(rows, calls))

    result = fxmd.get_latest("usd", "inflation")

    assert result["latest"]["date"] == rows[0]["date"]
    assert len(result["observations"]) == 10
    assert [call["limit"] for call in calls] == [20]


def test_fxmacrodata_get_latest_returns_none_when_nothing_is_published(
    fxmacrodata_factory,
):
    calls = []
    rows = [
        {"date": "2025-01-14", "val": "1", "announcement_datetime": "2025-01-20T13:30:00Z"}
        for _ in range(30)
    ]
    fxmd = fxmacrodata_factory(_Strategy(), _paged_fake_get(rows, calls))

    result = fxmd.get_latest("usd", "inflation")

    assert result["latest"] is None
    assert result["observations"] == []
    assert [call["limit"] for call in calls] == [20, 100]


def test_fxmacrodata_backtest_latest_cache_is_refetched_if_it_stopped_early(
    fxmacrodata_factory,
):
    calls = []
    unpublished = [
        {"date": "2025-01-14", "val": "9", "announcement_datetime": "2025-01-15T13:00:00Z"}
        for _ in range(20)
    ]
    published = {
        "date": "2024-12-01",
        "val": "2.7",
        "announcement_datetime": "2025-01-10T13:30:00Z",
    }
    rows = [*unpublished, published]
    fxmd = fxmacrodata_factory(_BacktestingStrategy(), _paged_fake_get(rows, calls))

    # Later in the day the first page already holds a published row ...
    later = fxmd.get_latest("usd", "inflation", as_of="2025-01-15T14:00:00Z")
    # ... but earlier the same day that cached page is not enough.
    earlier = fxmd.get_latest("usd", "inflation", as_of="2025-01-15T12:00:00Z")

    assert later["latest"]["value"] == 9.0
    assert earlier["latest"]["value"] == 2.7
    assert [call["limit"] for call in calls] == [20, 20, 100]


def test_fxmacrodata_gates_on_epoch_announcement_datetime(fxmacrodata_factory):
    # December data released 2025-01-15 11:30 UTC; January data released 2025-02-14.
    released = int(datetime(2025, 1, 15, 11, 30, tzinfo=timezone.utc).timestamp())
    not_yet_released = int(datetime(2025, 2, 14, 13, 30, tzinfo=timezone.utc).timestamp())

    def fake_get(url, **kwargs):
        return _Response(
            payload={
                "data": [
                    {
                        "date": "2025-01-01",
                        "val": 2.9,
                        "announcement_datetime": not_yet_released,
                        "publication_time_status": "confirmed",
                    },
                    {
                        "date": "2024-12-01",
                        "val": 2.7,
                        "announcement_datetime": released,
                        "publication_time_status": "unverified",
                        "publication_time_precision": "unknown",
                    },
                ]
            }
        )

    fxmd = fxmacrodata_factory(_Strategy(), fake_get)

    result = fxmd.get_series("usd", "inflation")

    assert [row["date"] for row in result["observations"]] == ["2024-12-01"]
    row = result["observations"][0]
    assert row["announcement_datetime"] == "2025-01-15T11:30:00+00:00"
    assert row["publication_time_status"] == "unverified"
    assert row["publication_time_precision"] == "unknown"
    assert result["publication_time"]["publication_time_status_counts"] == {"unverified": 1}


def _monthly_rows(count):
    """Most-recent-first monthly rows, like the API returns them."""
    rows = []
    for index in range(count):
        year, month = divmod(12 * 2024 - index - 1, 12)
        rows.append({"date": f"{year}-{month + 1:02d}-01", "val": str(index)})
    return rows


def _paged_fake_get(rows, calls, *, dataset_version="v1"):
    def fake_get(url, **kwargs):
        calls.append(kwargs["params"])
        params = kwargs["params"]
        if params.get("dataset_version") not in (None, dataset_version):
            return _Response(status_code=409)
        limit = params["limit"]
        offset = params.get("offset", 0)
        page = rows[offset:offset + limit]
        has_more = offset + len(page) < len(rows)
        return _Response(
            payload={
                "dataset_version": dataset_version,
                "data": page,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "returned_count": len(page),
                    "total_count": len(rows),
                    "has_more": has_more,
                    "next_offset": offset + len(page) if has_more else None,
                },
            }
        )

    return fake_get


def test_fxmacrodata_follows_pagination_for_long_windows(fxmacrodata_factory):
    calls = []
    rows = _monthly_rows(250)
    fxmd = fxmacrodata_factory(_Strategy(), _paged_fake_get(rows, calls))

    result = fxmd.get_series("usd", "inflation", start="2000-01-01")

    assert len(result["observations"]) == 250
    assert result["observations"][0]["date"] == rows[-1]["date"]
    assert result["observations"][-1]["date"] == rows[0]["date"]
    assert [call["limit"] for call in calls] == [100, 100, 100]
    assert [call.get("offset") for call in calls] == [None, 100, 200]
    assert [call.get("dataset_version") for call in calls] == [None, "v1", "v1"]


def test_fxmacrodata_limit_stops_paging_early(fxmacrodata_factory):
    calls = []
    rows = _monthly_rows(250)
    fxmd = fxmacrodata_factory(_Strategy(), _paged_fake_get(rows, calls))

    result = fxmd.get_series("usd", "inflation", start="2000-01-01", limit=120)

    assert len(result["observations"]) == 120
    assert result["observations"][-1]["date"] == rows[0]["date"]
    assert [call["limit"] for call in calls] == [100, 20]


def test_fxmacrodata_restarts_pagination_when_dataset_changes(fxmacrodata_factory):
    calls = []
    rows = _monthly_rows(150)
    current = {"version": "v1"}

    def fake_get(url, **kwargs):
        if kwargs["params"].get("dataset_version") == "v1":
            # A new release lands between the first and second page.
            current["version"] = "v2"
        return _paged_fake_get(rows, calls, dataset_version=current["version"])(url, **kwargs)

    fxmd = fxmacrodata_factory(_Strategy(), fake_get)

    result = fxmd.get_series("usd", "inflation", start="2000-01-01")

    assert len(result["observations"]) == 150
    assert [call.get("dataset_version") for call in calls] == [None, "v1", None, "v2"]


def test_fxmacrodata_live_requests_bypass_disk_cache(fxmacrodata_factory, tmp_path):
    calls = []
    payloads = [
        {"data": [{"date": "2025-01-01", "val": "3.0"}]},
        {"data": [{"date": "2025-01-01", "val": "4.0"}]},
    ]

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(payload=payloads[len(calls) - 1])

    fxmd = fxmacrodata_factory(_Strategy(), fake_get)

    first = fxmd.get_latest("usd", "inflation")
    second = fxmd.get_latest("usd", "inflation")

    assert first["latest"]["value"] == 3.0
    assert second["latest"]["value"] == 4.0
    assert len(calls) == 2
    assert not list(tmp_path.rglob("*.json"))


def test_fxmacrodata_backtests_use_disk_cache(fxmacrodata_factory, tmp_path):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(
            payload={
                "data": [
                    {
                        "date": "2025-01-01",
                        "val": str(len(calls)),
                        "announcement_datetime": "2025-01-10T13:30:00Z",
                    }
                ]
            }
        )

    fxmd = fxmacrodata_factory(_BacktestingStrategy(), fake_get)

    first = fxmd.get_latest("usd", "inflation")
    second = fxmd.get_latest("usd", "inflation")

    assert first["latest"]["value"] == 1.0
    assert second["latest"]["value"] == 1.0
    assert len(calls) == 1
    assert len(list(tmp_path.rglob("*.json"))) == 1
    assert not list(tmp_path.rglob("*.tmp"))


def test_fxmacrodata_usd_requests_do_not_require_api_key(fxmacrodata_factory):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(payload={"data": [{"date": "2025-01-01", "val": "3.0"}]})

    fxmd = fxmacrodata_factory(_Strategy(), fake_get)

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


def test_fxmacrodata_snapshot_reports_per_indicator_errors(fxmacrodata_factory):
    def fake_get(url, **_kwargs):
        if url.endswith("/policy_rate"):
            raise RuntimeError("upstream unavailable")
        return _Response(payload={"data": [{"date": "2025-01-01", "val": "3.0"}]})

    fxmd = fxmacrodata_factory(_Strategy(), fake_get, api_key="test-fxmd-key")

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
