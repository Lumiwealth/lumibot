from types import SimpleNamespace

import pytest

from lumibot.strategies.strategy_executor import StrategyExecutor


def test_backtest_progress_payload_uses_cached_portfolio_value():
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.broker = SimpleNamespace(
        data_source=SimpleNamespace(
            _show_progress_bar=True,
            log_backtest_progress_to_file=False,
        )
    )

    def fail_fresh_valuation():
        raise AssertionError("progress logging must not force fresh valuation")

    executor.strategy = SimpleNamespace(
        cash=1300.0,
        portfolio_value=1299.5,
        get_portfolio_value=fail_fresh_valuation,
    )

    payload = executor._build_backtest_progress_payload()

    assert payload == {"cash": 1300.0, "portfolio_value": 1299.5}


def test_backtest_progress_payload_falls_back_to_cash_when_cached_value_missing():
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.broker = SimpleNamespace(
        data_source=SimpleNamespace(
            _show_progress_bar=True,
            log_backtest_progress_to_file=False,
        )
    )
    executor.strategy = SimpleNamespace(cash=1300.0, portfolio_value=None)

    payload = executor._build_backtest_progress_payload()

    assert payload == {"cash": 1300.0, "portfolio_value": 1300.0}
