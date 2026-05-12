from __future__ import annotations

from datetime import datetime

import pandas as pd

from lumibot.tools.indicators import create_tearsheet


def test_tearsheet_retries_on_gaussian_kde_failure(monkeypatch, tmp_path):
    import quantstats_lumi as qs

    calls = {"n": 0}

    def fake_html(returns, benchmark=None, title=None, output=None, download_filename=None, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError(
                "The data appears to lie in a lower-dimensional subspace ... singular data covariance matrix ... gaussian_kde"
            )

        # Simulate QuantStats writing the report and returning a headline table.
        if output:
            with open(output, "w", encoding="utf-8") as f:
                f.write("<html><body>ok</body></html>")
        return pd.DataFrame(
            {
                "Metric": ["Total Return", "CAGR% (Annual Return)", "Max Drawdown"],
                "Strategy": ["1.00%", "1.00%", "-1.00%"],
            }
        )

    monkeypatch.setattr(qs.reports, "html", fake_html)

    idx = pd.date_range(datetime(2025, 12, 8), periods=5, freq="1D")
    strategy_df = pd.DataFrame({"portfolio_value": [100, 101, 99, 102, 103]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [100, 100.5, 100.2, 101, 101.1]}, index=idx)

    out = tmp_path / "tearsheet.html"
    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="TestStrategy",
        tearsheet_file=str(out),
        benchmark_df=benchmark_df,
        benchmark_asset="MES",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        strategy_parameters={},
        lumibot_version="dev",
        backtesting_data_source="ibkr",
        backtesting_data_sources="ibkr",
        backtest_time_seconds=1.0,
    )

    assert calls["n"] == 2
    text = out.read_text(encoding="utf-8")
    assert "Tearsheet not generated" not in text
