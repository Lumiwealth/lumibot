from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from lumibot.strategies._strategy import _Strategy


class _HookHarness:
    _extract_returns_series = staticmethod(_Strategy._extract_returns_series)
    _build_drawdown_inputs = staticmethod(_Strategy._build_drawdown_inputs)
    _default_cash_tearsheet_metrics = _Strategy._default_cash_tearsheet_metrics
    _collect_custom_tearsheet_metrics = _Strategy._collect_custom_tearsheet_metrics


def test_collect_custom_tearsheet_metrics_passes_expected_inputs():
    idx = pd.date_range(datetime(2025, 1, 1), periods=5, freq="1D")
    strategy_returns_df = pd.DataFrame(
        {
            "portfolio_value": [100000.0, 100500.0, 100250.0, 101000.0, 100700.0],
        },
        index=idx,
    )
    strategy_returns_df["return"] = strategy_returns_df["portfolio_value"].pct_change(fill_method=None)

    benchmark_df = pd.DataFrame({"symbol_cumprod": [100.0, 100.2, 100.0, 100.5, 100.4]}, index=idx)
    benchmark_df["return"] = benchmark_df["symbol_cumprod"].pct_change(fill_method=None)

    captured: dict[str, object] = {}

    def hook(*, stats_df, strategy_returns, benchmark_returns, drawdown, drawdown_details, risk_free_rate):
        captured["stats_df"] = stats_df
        captured["strategy_returns"] = strategy_returns
        captured["benchmark_returns"] = benchmark_returns
        captured["drawdown"] = drawdown
        captured["drawdown_details"] = drawdown_details
        captured["risk_free_rate"] = risk_free_rate
        return {"Custom Metric": 7.5}

    harness = _HookHarness()
    harness._stats = strategy_returns_df.copy(deep=True)
    harness._strategy_returns_df = strategy_returns_df
    harness._benchmark_returns_df = benchmark_df
    harness.risk_free_rate = 0.03
    harness.logger = logging.getLogger("test_tearsheet_hook")
    harness.tearsheet_custom_metrics = hook

    result = harness._collect_custom_tearsheet_metrics()

    assert result == {"Custom Metric": 7.5}
    assert isinstance(captured["stats_df"], pd.DataFrame)
    assert isinstance(captured["strategy_returns"], pd.Series)
    assert isinstance(captured["drawdown"], pd.Series)
    assert isinstance(captured["drawdown_details"], pd.DataFrame)
    assert captured["risk_free_rate"] == 0.03


def test_collect_custom_tearsheet_metrics_rejects_non_dict_return():
    idx = pd.date_range(datetime(2025, 1, 1), periods=3, freq="1D")
    strategy_returns_df = pd.DataFrame({"portfolio_value": [100.0, 101.0, 102.0]}, index=idx)
    strategy_returns_df["return"] = strategy_returns_df["portfolio_value"].pct_change(fill_method=None)

    harness = _HookHarness()
    harness._stats = strategy_returns_df.copy(deep=True)
    harness._strategy_returns_df = strategy_returns_df
    harness._benchmark_returns_df = None
    harness.risk_free_rate = 0.0
    harness.logger = logging.getLogger("test_tearsheet_hook")
    harness.tearsheet_custom_metrics = lambda **kwargs: ["invalid", "shape"]

    result = harness._collect_custom_tearsheet_metrics()

    assert result == {}
