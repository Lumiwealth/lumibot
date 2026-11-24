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


def test_backtest_entry_starts_the_prewarm():
    import lumibot.strategies._strategy as strategy_module

    source = open(strategy_module.__file__, encoding="utf-8").read()
    # The call sits inside run_backtest, before the trader runs, guarded by the report flags.
    run_backtest = source[source.index("    def run_backtest("):]
    run_backtest = run_backtest[: run_backtest.index("self._trader.run_all(")]
    assert "prewarm_report_imports()" in run_backtest
