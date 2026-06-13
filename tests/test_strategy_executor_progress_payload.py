from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from lumibot.strategies.strategy_executor import StrategyExecutor


class _NoTelemetryStrategy:
    __test__ = False

    name = "test"

    @property
    def cash(self):
        raise AssertionError("cash should not be read when progress outputs are disabled")

    def get_portfolio_value(self):
        raise AssertionError("portfolio value should not be read when progress outputs are disabled")

    def get_positions(self):
        raise AssertionError("positions should not be serialized when progress outputs are disabled")

    def _update_cash_with_dividends(self):
        self.dividends_updated = True


def test_build_backtest_progress_payload_progress_only_skips_snapshot_serialization():
    def fail_fresh_valuation():
        raise AssertionError("progress payload should use cached portfolio value")

    strategy = SimpleNamespace(
        cash=100.0,
        portfolio_value=123.0,
        get_portfolio_value=fail_fresh_valuation,
        get_positions=lambda: (_ for _ in ()).throw(AssertionError("positions should not be serialized")),
        name="test",
    )
    broker = SimpleNamespace(
        data_source=SimpleNamespace(_show_progress_bar=True, log_backtest_progress_to_file=False),
    )
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = strategy
    executor.broker = broker

    payload = executor._build_backtest_progress_payload()

    assert payload == {"cash": 100.0, "portfolio_value": 123.0}


def test_process_pandas_daily_data_skips_progress_payload_when_outputs_disabled():
    dates = pd.DatetimeIndex(["2025-01-02", "2025-01-03"])
    data_source = SimpleNamespace(
        _date_index=dates,
        _iter_count=None,
        _show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    update_calls = []
    broker = SimpleNamespace(
        data_source=data_source,
        datetime=pd.Timestamp("2025-01-01"),
        IS_BACKTESTING_BROKER=True,
        _update_datetime=lambda dt, **kwargs: update_calls.append((dt, kwargs)),
        process_pending_orders=lambda strategy=None: update_calls.append(("processed", strategy)),
    )
    strategy = _NoTelemetryStrategy()
    strategy.dividends_updated = False
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = strategy
    executor.broker = broker
    executor.stop_event = SimpleNamespace(set=lambda: update_calls.append(("stopped", None)))
    executor._on_trading_iteration = lambda: update_calls.append(("iteration", None))

    executor._process_pandas_daily_data()

    assert update_calls[0] == (dates[0], {})
    assert ("iteration", None) in update_calls
    assert ("processed", strategy) in update_calls
    assert strategy.dividends_updated is True


def test_build_backtest_progress_payload_falls_back_on_snapshot_errors():
    def fail_fresh_valuation():
        raise AssertionError("progress payload should use cached portfolio value")

    strategy = SimpleNamespace(
        cash=100.0,
        portfolio_value=123.0,
        get_portfolio_value=fail_fresh_valuation,
        get_positions=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        _initial_budget=1000.0,
        name="test",
    )
    broker = SimpleNamespace(
        data_source=SimpleNamespace(
            _show_progress_bar=False,
            log_backtest_progress_to_file=True,
            _last_logging_time=None,
        ),
        get_tracked_orders=lambda strategy=None: (_ for _ in ()).throw(AssertionError("should not be reached")),
    )
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = strategy
    executor.broker = broker

    payload = executor._build_backtest_progress_payload()

    assert payload == {
        "cash": 100.0,
        "portfolio_value": 123.0,
        "initial_budget": 1000.0,
        "positions": None,
        "orders": None,
    }
