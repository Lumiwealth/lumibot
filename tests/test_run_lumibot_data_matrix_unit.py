from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_lumibot_data_matrix.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_lumibot_data_matrix_script", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_window_to_start_supports_years_and_days():
    module = _load_module()
    end_dt = datetime(2026, 4, 10, tzinfo=timezone.utc)

    assert module._window_to_start(end_dt, "30d") == datetime(2026, 3, 11, tzinfo=timezone.utc)
    assert module._window_to_start(end_dt, "1y") == datetime(2025, 4, 10, tzinfo=timezone.utc)


def test_backtest_bounds_end_at_requested_timestamp():
    module = _load_module()
    requested_end = datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc)

    minute_start, minute_end = module._backtest_bounds_for_requested_end(requested_end=requested_end, interval="minute")
    hour_start, hour_end = module._backtest_bounds_for_requested_end(requested_end=requested_end, interval="hour")
    day_start, day_end = module._backtest_bounds_for_requested_end(requested_end=requested_end, interval="day")

    assert minute_start == datetime(2026, 4, 10, 18, 0, tzinfo=timezone.utc)
    assert minute_end == requested_end
    assert hour_start == datetime(2026, 4, 10, 18, 0, tzinfo=timezone.utc)
    assert hour_end == requested_end
    assert day_start == datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc)
    assert day_end == requested_end


def test_normalize_case_status_upgrades_legacy_completed_and_error_cases():
    module = _load_module()

    completed = module._normalize_case_status({"status": "completed", "outcome": None})
    assert completed["status"] == "pass"
    assert completed["outcome"] == "pass"

    support_limit = module._normalize_case_status(
        {
            "status": "error",
            "outcome": None,
            "error": "UnavailabeTimestep: YAHOO data source does not have data with 'hour' timestep",
        }
    )
    assert support_limit["status"] == "support-limit"
    assert support_limit["outcome"] == "support-limit"

    failed = module._normalize_case_status({"status": "error", "outcome": None, "error": "RuntimeError: bad"})
    assert failed["status"] == "fail"
    assert failed["outcome"] == "fail"


def test_fetch_exact_window_frame_falls_back_to_public_history_slice():
    module = _load_module()
    asset = module.Asset("VIX", asset_type=module.Asset.AssetType.INDEX)
    idx = pd.DatetimeIndex(
        [
            datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 9, 20, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
        ]
    )
    df = pd.DataFrame(
        {
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.5, 12.5],
            "low": [9.5, 10.5, 11.5],
            "close": [10.2, 11.2, 12.2],
        },
        index=idx,
    )

    class _Bars:
        def __init__(self, frame):
            self.df = frame

    class _Strategy:
        def __init__(self):
            self.broker = type("_Broker", (), {"data_source": object()})()

        def get_historical_prices(self, asset, length, timestep, quote=None):
            return _Bars(df)

    frame = module._fetch_exact_window_frame(
        strategy=_Strategy(),
        asset=asset,
        quote=None,
        interval="day",
        start_dt=datetime(2026, 4, 9, 20, 0, tzinfo=timezone.utc),
        end_dt=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
        exchange=None,
    )

    assert list(frame.index) == list(idx[1:])
    assert list(frame["close"]) == [11.2, 12.2]


def test_windows_for_ibkr_prefer_downloader_targets():
    module = _load_module()
    targets = {
        ("VIX", "minute"): {
            "max_reachable_window": "180d",
            "first_support_limit_window": "1y",
        }
    }

    assert module._windows_for_cell(provider="ibkr", symbol="VIX", interval="minute", downloader_targets=targets) == [
        "180d",
        "1y",
    ]
    assert module._windows_for_cell(provider="yahoo", symbol="VIX", interval="minute", downloader_targets=targets) == module.WINDOWS_BY_INTERVAL["minute"]


def test_quality_issues_detect_duplicates_and_coverage_shortfall():
    module = _load_module()
    idx = pd.DatetimeIndex(
        [
            datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            datetime(2026, 4, 10, 19, 57, tzinfo=timezone.utc),
            datetime(2026, 4, 10, 19, 57, tzinfo=timezone.utc),
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [10.0, 10.0, 10.0],
            "high": [10.0, 10.0, 10.0],
            "low": [10.0, 10.0, 10.0],
            "close": [10.0, 10.0, 10.0],
        },
        index=idx,
    )

    issues, coverage_ok = module._quality_issues_for_frame(
        frame=frame,
        interval="minute",
        asset_type="index",
        requested_start=datetime(2026, 4, 10, 19, 30, tzinfo=timezone.utc),
        requested_end=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
    )

    assert coverage_ok is False
    assert "duplicate_timestamps:1" in issues
    assert "coverage_shortfall" in issues


def test_summarize_interval_reports_support_limit_with_artifact():
    module = _load_module()
    downloader_targets = {
        ("VIX", "minute"): {
            "backend": "tws",
            "classification": "complete",
            "cache_write_policy": "replace",
        }
    }
    cases = [
        {
            "provider": "ibkr",
            "symbol": "VIX",
            "asset_type": "index",
            "interval": "minute",
            "window": "180d",
            "outcome": "pass",
            "backend": "tws",
            "classification": "complete",
            "cache_write_policy": "replace",
            "row_count": 100,
            "first_timestamp": "2025-10-01T00:00:00+00:00",
            "last_timestamp": "2026-04-09T20:00:00+00:00",
            "quality_issues": [],
            "artifact_path": "/tmp/vix-minute.html",
            "metadata_path": "/tmp/vix-minute.json",
            "validation_log_path": "/tmp/vix-minute.log",
        },
        {
            "provider": "ibkr",
            "symbol": "VIX",
            "asset_type": "index",
            "interval": "minute",
            "window": "1y",
            "outcome": "support-limit",
            "backend": "tws",
            "classification": "partial",
            "cache_write_policy": "replace",
            "row_count": 100,
            "first_timestamp": "2025-10-01T00:00:00+00:00",
            "last_timestamp": "2026-04-09T20:00:00+00:00",
            "quality_issues": ["coverage_shortfall"],
            "artifact_path": "/tmp/vix-minute-limit.html",
            "metadata_path": "/tmp/vix-minute-limit.json",
            "validation_log_path": "/tmp/vix-minute-limit.log",
        },
    ]

    summary = module._summarize_interval(
        provider="ibkr",
        symbol="VIX",
        asset_type="index",
        interval="minute",
        cases=cases,
        downloader_targets=downloader_targets,
    )

    assert summary["status"] == "support-limit"
    assert summary["confidence"] == "proven but shallow"
    assert summary["backend"] == "tws"
    assert summary["artifact_path"] == "/tmp/vix-minute.html"


def test_summarize_interval_marks_explicit_support_limit_as_proven_when_no_passes_exist():
    module = _load_module()
    summary = module._summarize_interval(
        provider="yahoo",
        symbol="VIX",
        asset_type="index",
        interval="hour",
        cases=[
            {
                "provider": "yahoo",
                "symbol": "VIX",
                "asset_type": "index",
                "interval": "hour",
                "window": "5d",
                "outcome": "support-limit",
                "status": "support-limit",
                "error": "UnavailabeTimestep: YAHOO data source does not have data with 'hour' timestep",
                "quality_issues": [],
                "artifact_path": "/tmp/vix-hour-limit.html",
                "metadata_path": "/tmp/vix-hour-limit.json",
                "validation_log_path": "/tmp/vix-hour-limit.log",
            }
        ],
        downloader_targets={},
    )

    assert summary["status"] == "support-limit"
    assert summary["confidence"] == "proven"


def test_write_runner_state_persists_current_case_and_report_path(tmp_path):
    module = _load_module()
    outdir = tmp_path / "out"
    outdir.mkdir()
    report_path = outdir / "lumibot_matrix.json"

    module._write_runner_state(
        outdir=outdir,
        state="running",
        current_case={
            "provider": "ibkr",
            "symbol": "VIX",
            "interval": "minute",
            "window": "3y",
        },
        report_path=report_path,
    )

    payload = json.loads((outdir / "lumibot_runner_state.json").read_text(encoding="utf-8"))
    assert payload["state"] == "running"
    assert payload["report_path"] == str(report_path)
    assert payload["current_case"]["symbol"] == "VIX"
    assert payload["completed_case"] is None
