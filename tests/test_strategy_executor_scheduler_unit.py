from types import SimpleNamespace

from lumibot.strategies.strategy_executor import _BacktestSchedulerStub


def test_backtest_scheduler_stub_materialize_reuses_existing_scheduler():
    executor = SimpleNamespace(scheduler=None)
    stub = _BacktestSchedulerStub(executor)
    materialized_scheduler = object()
    executor.scheduler = materialized_scheduler

    assert stub._materialize() is materialized_scheduler
