"""A scheduler worker may inspect jobs while another thread stops the strategy."""

from datetime import datetime, timezone
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

from lumibot.strategies.strategy_executor import StrategyExecutor


def test_shutdown_joins_running_jobs_after_scheduler_releases_its_jobs_lock(monkeypatch):
    events = []
    strategy = SimpleNamespace(
        broker=SimpleNamespace(IS_BACKTESTING_BROKER=False,
                               _close_connection=lambda: events.append("broker_closed")),
        backup_variables_to_db=lambda: events.append("variables_saved"),
        on_trading_iteration=Mock(),
    )
    executor = StrategyExecutor(strategy)
    scheduler = executor.ensure_scheduler()
    worker_started, read_jobs = Event(), Event()

    def job():
        worker_started.set()
        read_jobs.wait()
        executor.get_next_ap_scheduler_run_time()
        events.append("job_finished")

    scheduler.add_job(job, "date", run_date=datetime.now(timezone.utc))
    scheduler.start()
    worker_started.wait()
    pool = scheduler._executors["default"]
    original_shutdown = pool.shutdown
    unsafe_waits = []

    def observe_shutdown(wait=True):
        if wait and scheduler._jobstores_lock._is_owned():
            # Detect the exact lock cycle without leaving a deadlocked test worker.
            unsafe_waits.append(True)
            return
        if wait:
            read_jobs.set()
        original_shutdown(wait)

    monkeypatch.setattr(pool, "shutdown", observe_shutdown)
    try:
        executor.gracefully_exit()
        assert unsafe_waits == [], "Joining under the jobs lock blocks a worker reading its next run"
        assert events == ["job_finished", "broker_closed", "variables_saved"]
        assert executor.scheduler is None
        assert not scheduler.running
        executor.gracefully_exit()
    finally:
        read_jobs.set()
        if scheduler.running:
            scheduler.shutdown(wait=False)
        original_shutdown(wait=True)


def test_backtest_exit_does_not_create_a_scheduler():
    strategy = SimpleNamespace(
        broker=SimpleNamespace(IS_BACKTESTING_BROKER=True),
        _dump_stats=Mock(), backup_variables_to_db=Mock(), on_trading_iteration=Mock(),
    )
    executor = StrategyExecutor(strategy)
    executor.gracefully_exit()
    assert executor.scheduler is None
    strategy._dump_stats.assert_called_once_with()
    strategy.backup_variables_to_db.assert_called_once_with()
