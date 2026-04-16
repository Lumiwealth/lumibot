from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_release_readiness.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("build_release_readiness_script", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_merge_lumibot_reports_replaces_duplicate_cells_with_latest_report():
    module = _load_module()
    merged = module._merge_lumibot_reports(
        [
            (Path("/tmp/first.json"), {
                "lumibot_file": "/repo/lumibot/__init__.py",
                "lumibot_matrix": [
                    {
                        "provider": "ibkr",
                        "symbol": "VIX",
                        "interval": "day",
                        "status": "fail",
                        "artifact_path": "/tmp/old.html",
                    }
                ],
                "cases": [],
                "failure_buckets": [],
                "unsupported_limits": [],
            }),
            (Path("/tmp/second.json"), {
                "lumibot_matrix": [
                    {
                        "provider": "ibkr",
                        "symbol": "VIX",
                        "interval": "day",
                        "status": "pass",
                        "artifact_path": "/tmp/new.html",
                    }
                ],
                "cases": [],
                "failure_buckets": [],
                "unsupported_limits": [],
            }),
        ]
    )

    row = merged["lumibot_matrix"][0]
    assert row["status"] == "pass"
    assert row["artifact_path"] == "/tmp/new.html"
    assert merged["lumibot_file"] == "/repo/lumibot/__init__.py"


def test_merge_lumibot_reports_rewrites_relocated_artifact_paths(tmp_path):
    module = _load_module()
    report_dir = tmp_path / "lumibot_yahoo_matrix_prodfix_2026-04-12"
    artifact_dir = report_dir / "artifacts" / "yahoo" / "VIX" / "day" / "30y"
    artifact_dir.mkdir(parents=True)
    indicators = artifact_dir / "indicators.html"
    indicators.write_text("<html></html>", encoding="utf-8")

    merged = module._merge_lumibot_reports(
        [
            (
                report_dir / "lumibot_matrix.json",
                {
                    "lumibot_matrix": [
                        {
                            "provider": "yahoo",
                            "symbol": "VIX",
                            "interval": "day",
                            "status": "pass",
                            "artifact_path": "/old/outdir/artifacts/yahoo/VIX/day/30y/indicators.html",
                        }
                    ],
                    "cases": [],
                    "failure_buckets": [],
                    "unsupported_limits": [],
                },
            )
        ]
    )

    row = merged["lumibot_matrix"][0]
    assert row["artifact_path"] == str(indicators.resolve())


def test_compute_release_readiness_flags_missing_cells_and_artifacts(tmp_path):
    module = _load_module()
    artifact_root = tmp_path / "release_artifacts"
    artifact_root.mkdir()
    downloader_rows = [
        {
            "provider": "ibkr",
            "symbol": symbol,
            "interval": interval,
            "status": "pass",
        }
        for symbol in module.REQUIRED_IBKR_SYMBOLS
        for interval in module.REQUIRED_INTERVALS
    ]
    lumibot_rows = [
        {
            "provider": "ibkr",
            "symbol": symbol,
            "interval": interval,
            "status": "pass",
            "artifact_path": str((artifact_root / f"{symbol}-{interval}.html").resolve()) if symbol in module.REQUIRED_IBKR_INDEXES else "",
        }
        for symbol in module.REQUIRED_IBKR_SYMBOLS
        for interval in module.REQUIRED_INTERVALS
    ]
    for row in lumibot_rows:
        if row["artifact_path"]:
            Path(row["artifact_path"]).write_text("<html></html>", encoding="utf-8")
    lumibot_rows.extend(
        {
            "provider": "yahoo",
            "symbol": symbol,
            "interval": interval,
            "status": "support-limit",
            "artifact_path": str((artifact_root / f"yahoo-{symbol}-{interval}.html").resolve()),
        }
        for symbol in module.REQUIRED_YAHOO_SYMBOLS
        for interval in module.REQUIRED_INTERVALS
    )
    for row in lumibot_rows:
        if row["artifact_path"]:
            Path(row["artifact_path"]).write_text("<html></html>", encoding="utf-8")

    readiness = module._compute_release_readiness(
        downloader_rows=downloader_rows,
        lumibot_rows=lumibot_rows,
    )

    assert readiness["downloader"]["green"] == len(module._required_downloader_keys())
    assert readiness["lumibot"]["green"] == len(module._required_lumibot_keys())
    assert readiness["ready_to_think_about_deployment"] is False
    assert readiness["artifacts"]["missing_representative_stock_artifacts"] == ["minute", "hour", "day"]
    assert readiness["artifacts"]["missing_representative_crypto_artifacts"] == ["minute", "hour", "day"]


def test_artifact_present_requires_existing_file():
    module = _load_module()
    assert module._artifact_present({"artifact_path": "/definitely/not/here.html"}) is False
