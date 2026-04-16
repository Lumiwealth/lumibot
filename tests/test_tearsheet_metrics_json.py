from __future__ import annotations

import json
from datetime import datetime

import pandas as pd

from lumibot.tools.indicators import create_tearsheet


def test_create_tearsheet_writes_summary_metrics_json(monkeypatch, tmp_path):
    import quantstats_lumi as qs

    captured: dict[str, object] = {}

    def fake_html(returns, benchmark=None, title=None, output=None, download_filename=None, **kwargs):
        captured["html_custom_metrics"] = kwargs.get("custom_metrics")
        if output:
            with open(output, "w", encoding="utf-8") as handle:
                handle.write("<html><body>ok</body></html>")
        return pd.DataFrame(
            {
                "Metric": ["Total Return", "CAGR% (Annual Return)", "Max Drawdown"],
                "Strategy": ["1.00%", "1.00%", "-1.00%"],
            }
        )

    def fake_metrics_json(returns, benchmark=None, rf=0.0, output=None, summary_only=False, **kwargs):
        captured["metrics_summary_only"] = summary_only
        captured["metrics_custom_metrics"] = kwargs.get("custom_metrics")
        payload = {"metadata": {"summary_only": bool(summary_only)}, "scalar_metrics": {"Sharpe": 1.23}}
        if output:
            with open(output, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
        return payload

    monkeypatch.setattr(qs.reports, "html", fake_html)
    monkeypatch.setattr(qs.reports, "metrics_json", fake_metrics_json, raising=False)

    idx = pd.date_range(datetime(2025, 12, 8), periods=5, freq="1D")
    strategy_df = pd.DataFrame({"portfolio_value": [100, 101, 99, 102, 103]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [100, 100.5, 100.2, 101, 101.1]}, index=idx)

    tearsheet_file = tmp_path / "tearsheet.html"
    metrics_file = tmp_path / "tearsheet_metrics.json"
    custom_metrics = {"Custom Edge Score": 42.0}

    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="TestStrategy",
        tearsheet_file=str(tearsheet_file),
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
        tearsheet_metrics_file=str(metrics_file),
        custom_metrics=custom_metrics,
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()
    assert captured["metrics_summary_only"] is True
    assert captured["html_custom_metrics"] == custom_metrics
    assert captured["metrics_custom_metrics"] == custom_metrics

    written = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert written["metadata"]["summary_only"] is True
    assert written["scalar_metrics"]["Sharpe"] == 1.23


def test_create_tearsheet_writes_placeholder_metrics_json_on_degenerate_returns(tmp_path):
    idx = pd.to_datetime(["2025-01-02", "2025-01-03"])
    strategy_df = pd.DataFrame({"portfolio_value": [100000.0, 100000.0]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [1.0, 1.0]}, index=idx)

    tearsheet_file = tmp_path / "tearsheet.html"
    metrics_file = tmp_path / "tearsheet_metrics.json"
    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="FlatStrategy",
        tearsheet_file=str(tearsheet_file),
        benchmark_df=benchmark_df,
        benchmark_asset="SPY",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        tearsheet_metrics_file=str(metrics_file),
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()
    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert payload["metadata"]["summary_only"] is True
    assert payload["metadata"]["status"] == "unavailable"
    assert payload["metadata"]["reason"] == "degenerate_returns"


def test_create_tearsheet_writes_metrics_json_without_custom_metrics(monkeypatch, tmp_path):
    import quantstats_lumi as qs

    def fake_html(returns, benchmark=None, title=None, output=None, download_filename=None, **kwargs):
        if output:
            with open(output, "w", encoding="utf-8") as handle:
                handle.write("<html><body>ok</body></html>")
        return pd.DataFrame({"Metric": ["Sharpe"], "Strategy": ["1.23"]})

    def fake_metrics_json(returns, benchmark=None, rf=0.0, output=None, summary_only=False, **kwargs):
        payload = {"metadata": {"summary_only": bool(summary_only)}, "scalar_metrics": {"Sharpe": 1.23}}
        if output:
            with open(output, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
        return payload

    monkeypatch.setattr(qs.reports, "html", fake_html)
    monkeypatch.setattr(qs.reports, "metrics_json", fake_metrics_json, raising=False)

    idx = pd.date_range(datetime(2025, 12, 8), periods=5, freq="1D")
    strategy_df = pd.DataFrame({"portfolio_value": [100, 101, 100, 102, 103]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [100, 100.5, 100.2, 101, 101.1]}, index=idx)

    tearsheet_file = tmp_path / "tearsheet.html"
    metrics_file = tmp_path / "tearsheet_metrics.json"

    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="NoCustomMetricsStrategy",
        tearsheet_file=str(tearsheet_file),
        benchmark_df=benchmark_df,
        benchmark_asset="SPY",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        tearsheet_metrics_file=str(metrics_file),
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()
    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert "Sharpe" in payload["scalar_metrics"]


def test_create_tearsheet_falls_back_when_metrics_json_api_is_missing(monkeypatch, tmp_path):
    import quantstats_lumi as qs

    def fake_html(returns, benchmark=None, title=None, output=None, download_filename=None, **kwargs):
        if output:
            with open(output, "w", encoding="utf-8") as handle:
                handle.write("<html><body>ok</body></html>")
        return pd.DataFrame({"Metric": ["Sharpe"], "Strategy": ["1.23"]})

    def fake_metrics(returns, benchmark=None, rf=0.0, display=True, **kwargs):
        assert display is False
        return pd.DataFrame(
            {
                "Metric": ["Sharpe", "Total Return"],
                "Benchmark": [0.91, "4.00%"],
                "Strategy": [1.23, "8.00%"],
            }
        )

    monkeypatch.setattr(qs.reports, "html", fake_html)
    monkeypatch.delattr(qs.reports, "metrics_json", raising=False)
    monkeypatch.setattr(qs.reports, "metrics", fake_metrics)

    idx = pd.date_range(datetime(2025, 12, 8), periods=5, freq="1D")
    strategy_df = pd.DataFrame({"portfolio_value": [100, 101, 100, 102, 103]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [100, 100.5, 100.2, 101, 101.1]}, index=idx)

    tearsheet_file = tmp_path / "tearsheet.html"
    metrics_file = tmp_path / "tearsheet_metrics.json"

    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="FallbackMetricsStrategy",
        tearsheet_file=str(tearsheet_file),
        benchmark_df=benchmark_df,
        benchmark_asset="SPY",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        tearsheet_metrics_file=str(metrics_file),
    )

    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert payload["metadata"]["summary_only"] is True
    assert payload["metadata"]["status"] == "ok"
    assert payload["metadata"]["source"] == "metrics_fallback"
    assert payload["scalar_metrics"]["Sharpe"] == 1.23
    assert payload["scalar_metrics"]["Total Return"] == "8.00%"
    assert payload["benchmark_scalar_metrics"]["Sharpe"] == 0.91
    assert payload["benchmark_scalar_metrics"]["Total Return"] == "4.00%"


def test_create_tearsheet_writes_placeholder_metrics_json_on_insufficient_data(tmp_path):
    idx = pd.to_datetime(["2025-01-02"])
    strategy_df = pd.DataFrame({"portfolio_value": [100000.0]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [1.0]}, index=idx)

    tearsheet_file = tmp_path / "tearsheet.html"
    metrics_file = tmp_path / "tearsheet_metrics.json"
    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="InsufficientDataStrategy",
        tearsheet_file=str(tearsheet_file),
        benchmark_df=benchmark_df,
        benchmark_asset="SPY",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        tearsheet_metrics_file=str(metrics_file),
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()
    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert payload["metadata"]["summary_only"] is True
    assert payload["metadata"]["status"] == "unavailable"
    assert payload["metadata"]["reason"] in {"insufficient_data", "degenerate_returns"}
