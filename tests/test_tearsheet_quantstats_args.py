from __future__ import annotations

from datetime import datetime

import pandas as pd

from lumibot.tools.indicators import create_tearsheet


def _make_inputs(tmp_path):
    idx = pd.date_range(datetime(2025, 12, 8), periods=5, freq="1D")
    strategy_df = pd.DataFrame({"portfolio_value": [100, 101, 99, 102, 103]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [1.0, 1.005, 1.002, 1.01, 1.011]}, index=idx)
    return strategy_df, benchmark_df, tmp_path / "tearsheet.html"


def _base_kwargs(tearsheet_file, benchmark_asset):
    return dict(
        strat_name="TestStrategy",
        tearsheet_file=str(tearsheet_file),
        benchmark_asset=benchmark_asset,
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        strategy_parameters={},
        lumibot_version="dev",
        backtesting_data_source="yahoo",
        backtesting_data_sources="yahoo",
        backtest_time_seconds=1.0,
    )


def _fake_html_writer(output):
    with open(output, "w", encoding="utf-8") as f:
        f.write("<html><body>ok</body></html>")
    return pd.DataFrame({"Metric": ["Total Return"], "Strategy": ["1.00%"]})


def test_benchmark_series_name_passed_to_quantstats(monkeypatch, tmp_path):
    """Regression: benchmark label must be the asset symbol, not 'benchmark' / 'BENCHMARK'.

    df_final["benchmark"] returns a new Series on each access, so assigning to .name on
    that throwaway object is a no-op. The fix captures the series with .rename() before
    passing to qs.reports.html. Without the fix, QuantStats uppercases the column name
    and shows 'BENCHMARK' in the tearsheet instead of the actual symbol (e.g. 'SPY').
    """
    import quantstats_lumi as qs

    captured = {}

    def fake_html(returns, benchmark=None, **kwargs):
        captured["benchmark_name"] = getattr(benchmark, "name", None)
        return _fake_html_writer(kwargs.get("output"))

    monkeypatch.setattr(qs.reports, "html", fake_html)

    strategy_df, benchmark_df, out = _make_inputs(tmp_path)
    create_tearsheet(strategy_df=strategy_df, benchmark_df=benchmark_df, **_base_kwargs(out, "SPY"))

    assert captured.get("benchmark_name") == "SPY", (
        f"Expected benchmark Series name 'SPY' but got {captured.get('benchmark_name')!r}. "
        "The no-op .name assignment bug causes QuantStats to see name='benchmark' and "
        "uppercase it to 'BENCHMARK' in the tearsheet header."
    )


def test_benchmark_series_name_preserved_on_kde_retry(monkeypatch, tmp_path):
    """Regression: benchmark label must also be correct on the KDE-failure retry path.

    The fix assigns _benchmark_series once before both qs.reports.html call sites.
    This test verifies the retry path receives the same named Series as the primary path.
    """
    import quantstats_lumi as qs

    benchmark_names = []

    def fake_html(returns, benchmark=None, **kwargs):
        benchmark_names.append(getattr(benchmark, "name", None))
        if len(benchmark_names) == 1:
            raise RuntimeError("singular data covariance matrix ... gaussian_kde")
        return _fake_html_writer(kwargs.get("output"))

    monkeypatch.setattr(qs.reports, "html", fake_html)

    strategy_df, benchmark_df, out = _make_inputs(tmp_path)
    create_tearsheet(strategy_df=strategy_df, benchmark_df=benchmark_df, **_base_kwargs(out, "QQQ"))

    assert len(benchmark_names) == 2, f"Expected 2 qs.reports.html calls (primary + retry), got {len(benchmark_names)}"
    for i, name in enumerate(benchmark_names):
        assert name == "QQQ", (
            f"Call {i + 1}: expected benchmark Series name 'QQQ' but got {name!r}. "
            "The retry path must use _benchmark_series, not df_final['benchmark']."
        )
