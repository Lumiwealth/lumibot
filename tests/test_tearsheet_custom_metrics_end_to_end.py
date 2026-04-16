from __future__ import annotations

import json
from datetime import datetime

import pandas as pd

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from tests.fixtures import pandas_data_fixture


SPY = Asset(symbol="SPY", asset_type="stock")


class _CustomMetricsBacktestStrategy(Strategy):
    def initialize(self):
        self.asset = SPY
        self.sleeptime = "1D"
        self.vars.bought = False

    def on_trading_iteration(self):
        if not self.vars.bought:
            self.submit_order(self.create_order(self.asset, 1, "buy"))
            self.vars.bought = True

    def tearsheet_custom_metrics(
        self,
        stats_df,
        strategy_returns,
        benchmark_returns,
        drawdown,
        drawdown_details,
        risk_free_rate,
    ):
        non_null_returns = strategy_returns.dropna()
        avg_dd_days = (
            float(drawdown_details["days"].mean())
            if drawdown_details is not None
            and not drawdown_details.empty
            and "days" in drawdown_details.columns
            else 0.0
        )
        return {
            "Custom Return Observation Count": int(non_null_returns.shape[0]),
            "Custom Mean Absolute Daily Return": (
                float(non_null_returns.abs().mean()) if not non_null_returns.empty else 0.0
            ),
            "Custom Average Drawdown Days": avg_dd_days,
        }


class _PlainBacktestStrategy(Strategy):
    def initialize(self):
        self.asset = SPY
        self.sleeptime = "1D"
        self.vars.bought = False

    def on_trading_iteration(self):
        if not self.vars.bought:
            self.submit_order(self.create_order(self.asset, 1, "buy"))
            self.vars.bought = True


def _run_backtest_case(strategy_cls, pandas_data_fixture, case_dir, date_start, date_end):
    tearsheet_file = case_dir / "tearsheet.html"
    metrics_file = case_dir / "tearsheet_metrics.json"
    stats_file = case_dir / "stats.csv"

    results, strategy = strategy_cls.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=date_start,
        backtesting_end=date_end,
        pandas_data=pandas_data_fixture,
        benchmark_asset="SPY",
        budget=100000,
        risk_free_rate=0.0369,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=True,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        plot_file_html=str(case_dir / "trades.html"),
        stats_file=str(stats_file),
        tearsheet_file=str(tearsheet_file),
        indicators_file=str(case_dir / "indicators.html"),
        tearsheet_metrics_file=str(metrics_file),
    )
    return results, strategy, tearsheet_file, metrics_file, stats_file


def test_tearsheet_custom_metrics_end_to_end_backtest(pandas_data_fixture, tmp_path):
    _, strategy, tearsheet_file, metrics_file, stats_file = _run_backtest_case(
        _CustomMetricsBacktestStrategy,
        pandas_data_fixture,
        tmp_path / "custom_metrics_full_window",
        datetime(2019, 1, 2),
        datetime(2019, 3, 29),
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()
    assert stats_file.exists()

    strategy_returns = strategy._extract_returns_series(
        strategy._strategy_returns_df,
        returns_col="return",
        value_col="portfolio_value",
    )
    expected_count = int(strategy_returns.shape[0])
    expected_mean_abs = float(strategy_returns.abs().mean())

    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    scalar_metrics = payload["scalar_metrics"]
    assert scalar_metrics["Custom Return Observation Count"] == expected_count
    assert abs(float(scalar_metrics["Custom Mean Absolute Daily Return"]) - expected_mean_abs) < 1e-12
    assert "Custom Average Drawdown Days" in scalar_metrics

    html = tearsheet_file.read_text(encoding="utf-8")
    assert "Custom Return Observation Count" in html
    assert "Custom Mean Absolute Daily Return" in html
    assert "Custom Average Drawdown Days" in html


def test_tearsheet_end_to_end_backtest_without_custom_metrics(pandas_data_fixture, tmp_path):
    _, _, tearsheet_file, metrics_file, _ = _run_backtest_case(
        _PlainBacktestStrategy,
        pandas_data_fixture,
        tmp_path / "no_custom_metrics_full_window",
        datetime(2019, 1, 2),
        datetime(2019, 3, 29),
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()

    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    scalar_metrics = payload["scalar_metrics"]
    assert "Custom Return Observation Count" not in scalar_metrics
    assert "Custom Mean Absolute Daily Return" not in scalar_metrics

    html = tearsheet_file.read_text(encoding="utf-8")
    assert "Custom Return Observation Count" not in html
    assert "Custom Mean Absolute Daily Return" not in html


def test_tearsheet_end_to_end_backtest_short_window_writes_placeholder_metrics_json(
    pandas_data_fixture, tmp_path
):
    _, _, tearsheet_file, metrics_file, _ = _run_backtest_case(
        _CustomMetricsBacktestStrategy,
        pandas_data_fixture,
        tmp_path / "custom_metrics_short_window_edge_case",
        datetime(2019, 1, 2),
        datetime(2019, 1, 3),
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()

    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert payload["metadata"]["summary_only"] is True
    assert payload["metadata"]["status"] == "unavailable"
    assert payload["metadata"]["reason"] in {"insufficient_data", "degenerate_returns"}
