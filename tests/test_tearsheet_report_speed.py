"""End-of-run report speed: same numbers, less repeated work.

Why this exists: every backtest spends 8-14 seconds on reports in production. QuantStats
computes the drawdown table for the same drawdown series many times while it builds the
HTML tearsheet and the metrics JSON, and the QuantStats import itself (scipy, matplotlib)
only starts once the simulation has finished. These tests pin both fixes and prove the
report numbers do not change.
"""

from __future__ import annotations

import hashlib
import re
import sys
import threading
from datetime import datetime

import numpy as np
import pandas as pd

from lumibot.tools import report_speedups
from lumibot.tools.indicators import create_tearsheet


def _frames(days: int = 600, seed: int = 7):
    rng = np.random.default_rng(seed)
    idx = pd.date_range(datetime(2020, 1, 1), periods=days, freq="1D")
    strategy = 100_000 * np.cumprod(1 + rng.normal(0.0006, 0.02, days))
    bench = np.cumprod(1 + rng.normal(0.0004, 0.012, days))
    return (
        pd.DataFrame({"portfolio_value": strategy}, index=idx),
        pd.DataFrame({"symbol_cumprod": bench}, index=idx),
    )


def _run(tmp_path, name: str):
    strategy_df, benchmark_df = _frames()
    html_file = tmp_path / f"{name}.html"
    metrics_file = tmp_path / f"{name}.json"
    result = create_tearsheet(
        strategy_df=strategy_df,
        strat_name="SpeedTest",
        tearsheet_file=str(html_file),
        benchmark_df=benchmark_df,
        benchmark_asset="SPY",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        strategy_parameters={},
        lumibot_version="dev",
        backtesting_data_source="none",
        backtesting_data_sources="none",
        backtest_time_seconds=1.0,
        tearsheet_metrics_file=str(metrics_file),
    )
    return result, html_file, metrics_file


def _normalized_html(path) -> str:
    text = path.read_text(encoding="utf-8")
    # Matplotlib writes a timestamp and random element ids into every SVG.
    text = re.sub(r"<dc:date>.*?</dc:date>", "", text)
    text = re.sub(r"\b[mp][0-9a-f]{10}\b", "ID", text)
    return text


def test_drawdown_table_is_computed_once_per_distinct_series(tmp_path, monkeypatch):
    import quantstats_lumi.stats as qs_stats

    calls = {"n": 0}
    original = qs_stats.drawdown_details

    def counting(drawdown):
        calls["n"] += 1
        return original(drawdown)

    monkeypatch.setattr(qs_stats, "drawdown_details", counting)
    _run(tmp_path, "count")

    # QuantStats asks for the drawdown table of the same few series 13 times per report
    # (metrics twice, the worst-drawdowns table, the drawdown plot). Distinct inputs are
    # the strategy, the benchmark and their joint frame: at most 7 real computations.
    assert calls["n"] <= 7, calls["n"]
    # The report cache is scoped to one report: the module function is restored after.
    assert qs_stats.drawdown_details is counting


def test_period_aggregation_is_computed_once_per_distinct_input(tmp_path, monkeypatch):
    import quantstats_lumi.utils as qs_utils

    calls = {"n": 0}
    original = qs_utils.aggregate_returns

    def counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(qs_utils, "aggregate_returns", counting)
    _run(tmp_path, "aggregate")
    # 30 monthly/quarterly/yearly aggregations per report before, 11 distinct inputs.
    assert calls["n"] <= 11, calls["n"]
    assert qs_utils.aggregate_returns is counting


def test_aggregate_without_period_still_returns_the_callers_object():
    import quantstats_lumi.utils as qs_utils

    returns = pd.Series([0.01, -0.02, 0.03], index=pd.date_range(datetime(2021, 1, 1), periods=3, freq="1D"))
    with report_speedups.report_computation_cache():
        assert qs_utils.aggregate_returns(returns) is returns
        monthly_a = qs_utils.aggregate_returns(returns, "month", True)
        monthly_a.iloc[0] = 99.0
        monthly_b = qs_utils.aggregate_returns(returns, "month", True)
    assert monthly_b.iloc[0] != 99.0


def test_report_cache_keeps_every_report_number_identical(tmp_path, monkeypatch):
    cached = _run(tmp_path, "cached")

    monkeypatch.setattr(report_speedups, "drawdown_cache_enabled", lambda: False)
    plain = _run(tmp_path, "plain")

    assert cached[0].to_csv() == plain[0].to_csv()
    assert cached[2].read_bytes() == plain[2].read_bytes()
    assert hashlib.sha256(_normalized_html(cached[1]).encode()).hexdigest() == hashlib.sha256(
        _normalized_html(plain[1]).encode()
    ).hexdigest()


def test_cached_drawdown_table_cannot_be_mutated_by_a_caller():
    idx = pd.date_range(datetime(2021, 1, 1), periods=6, freq="1D")
    dd = pd.Series([0.0, -0.1, -0.2, 0.0, -0.05, 0.0], index=idx)
    import quantstats_lumi.stats as qs_stats

    with report_speedups.report_computation_cache():
        first = qs_stats.drawdown_details(dd)
        first.iloc[0, 3] = 999
        second = qs_stats.drawdown_details(dd)
    assert second.iloc[0, 3] != 999


def test_different_series_with_same_values_but_other_dates_are_not_shared():
    import quantstats_lumi.stats as qs_stats

    values = [0.0, -0.1, -0.2, 0.0, -0.05, 0.0]
    a = pd.Series(values, index=pd.date_range(datetime(2021, 1, 1), periods=6, freq="1D"))
    b = pd.Series(values, index=pd.date_range(datetime(2022, 3, 1), periods=6, freq="1D"))
    with report_speedups.report_computation_cache():
        da = qs_stats.drawdown_details(a)
        db = qs_stats.drawdown_details(b)
    assert str(da.iloc[0]["start"]).startswith("2021")
    assert str(db.iloc[0]["start"]).startswith("2022")


def test_prewarm_loads_the_report_stack_in_the_background(monkeypatch):
    for name in [m for m in list(sys.modules) if m == "quantstats_lumi" or m.startswith("quantstats_lumi.")]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    monkeypatch.setattr(report_speedups, "_prewarm_thread", None)

    thread = report_speedups.prewarm_report_imports()
    assert isinstance(thread, threading.Thread)
    assert thread.daemon
    report_speedups.wait_for_report_imports()
    assert "quantstats_lumi.reports" in sys.modules
    assert "quantstats_lumi.plots" in sys.modules
    # A second call reuses the same thread instead of starting another one.
    assert report_speedups.prewarm_report_imports() is thread


def test_prewarm_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("LUMIBOT_PREWARM_REPORT_IMPORTS", "0")
    monkeypatch.setattr(report_speedups, "_prewarm_thread", None)
    assert report_speedups.prewarm_report_imports() is None


def test_cache_setup_failure_runs_the_report_without_the_cache(monkeypatch):
    """A failed QuantStats import inside the cache setup must not escape or leave wrappers."""
    import importlib

    from lumibot.tools import report_speedups

    qs_stats = importlib.import_module("quantstats_lumi.stats")
    original = qs_stats.drawdown_details
    real_import = importlib.import_module

    def failing_import(name, *args, **kwargs):
        if name == "quantstats_lumi.utils":
            raise ImportError("simulated")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(report_speedups.importlib, "import_module", failing_import)
    ran = []
    with report_speedups.report_computation_cache():
        ran.append(True)
    assert ran == [True]
    assert qs_stats.drawdown_details is original


def test_report_prewarm_waits_for_the_first_simulation_step(monkeypatch):
    """Measured on a hosted 2 vCPU machine (4.6.98): starting the report import thread at
    backtest start delayed the first progress by about 2.4-3 s (8.7 and 9.4 s to the
    strategy's initialize with it, 6.4 s without), because the main thread waited on the
    same imports. The thread must start only once the simulation is running."""
    from lumibot.data_sources.data_source_backtesting import DataSourceBacktesting

    started = []
    monkeypatch.setattr(report_speedups, "_prewarm_thread", None)
    monkeypatch.setattr(report_speedups, "_prewarm_requested", False, raising=False)
    monkeypatch.setattr(report_speedups, "prewarm_report_imports", lambda: started.append(1))

    report_speedups.request_report_prewarm()
    assert started == []

    import types

    source = types.SimpleNamespace(
        _datetime=datetime(2026, 1, 5, 9, 30),
        _show_progress_bar=False,
        log_backtest_progress_to_file=False,
        record_runtime_milestone=lambda *args, **kwargs: None,
    )
    DataSourceBacktesting._update_datetime(source, datetime(2026, 1, 5, 9, 31))
    DataSourceBacktesting._update_datetime(source, datetime(2026, 1, 5, 9, 32))
    assert started == [1]
