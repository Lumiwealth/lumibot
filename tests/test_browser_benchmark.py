from lumibot.components.agents.browser_benchmark import (
    anti_detection_verdict,
    create_engine,
    summarize_samples,
)
from lumibot.components.agents.browser_tools import CamoufoxEngine, PatchrightEngine


def test_browser_benchmark_summary_reports_latency_memory_and_crashes():
    summary = summarize_samples(
        [
            {"open_seconds": 1.0, "rss_bytes": 100, "ok": True},
            {"open_seconds": 2.0, "rss_bytes": 300, "ok": True},
            {"open_seconds": 5.0, "rss_bytes": 200, "ok": False, "error": "boom"},
        ]
    )

    assert summary == {
        "iterations": 3,
        "passed": 2,
        "failed": 1,
        "crash_rate": 1 / 3,
        "open_seconds": {"min": 1.0, "p50": 2.0, "p95": 4.7, "max": 5.0},
        "peak_rss_bytes": 300,
        "errors": ["boom"],
    }


def test_browser_benchmark_selects_supported_engines():
    assert isinstance(create_engine("patchright"), PatchrightEngine)
    assert isinstance(create_engine("camoufox"), CamoufoxEngine)


def test_browser_benchmark_anti_detection_verdict_is_explicit():
    assert anti_detection_verdict(
        {"webdriver": False, "userAgent": "Mozilla/5.0 Firefox/152.0", "plugins": 5}
    ) == {"passed": True, "failures": []}
    assert anti_detection_verdict(
        {"webdriver": False, "userAgent": "HeadlessChrome/140", "plugins": 0}
    ) == {
        "passed": False,
        "failures": ["headless user agent", "no browser plugins"],
    }
