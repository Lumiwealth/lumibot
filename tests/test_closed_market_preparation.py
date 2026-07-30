import datetime
import logging
import os
import unittest
from types import SimpleNamespace
from unittest import mock

from lumibot.strategies.strategy_executor import (
    ClosedMarketOrderMutationError,
    StrategyExecutor,
)


class DummyBroker:
    IS_BACKTESTING_BROKER = False
    market = "NYSE"

    def __init__(self):
        self._first_iteration = True
        self._orders_queue = SimpleNamespace(queue=[])

    def is_backtesting_broker(self):
        return False

    def set_strategy_name(self, _name):
        return None

    def initialize_market_calendars(self, _trading_days):
        return None

    def is_market_open(self):
        return False

    def _close_connection(self):
        return None


class DummyStrategy:
    def __init__(self):
        self.broker = DummyBroker()
        self._name = "closed-market-preparation"
        self.parameters = {}
        self.is_backtesting = False
        self.logger = logging.getLogger("test_closed_market_preparation")
        self.sleeptime = "1D"
        self._analysis = {}
        self._first_iteration = True
        self._last_on_trading_iteration_datetime = None
        self.portfolio_value = 100
        self.cash = 100
        self.preparation_runs = 0
        self.trading_runs = 0
        self.end_runs = 0
        self.backups = 0

    def log_message(self, *_args, **_kwargs):
        return None

    def initialize(self):
        return None

    def on_closed_market_iteration(self):
        self.preparation_runs += 1

    def on_trading_iteration(self):
        self.trading_runs += 1

    def on_strategy_end(self):
        self.end_runs += 1

    def on_bot_crash(self, _error):
        return None

    def load_variables_from_db(self):
        return None

    def backup_variables_to_db(self):
        self.backups += 1

    def get_datetime(self):
        return datetime.datetime(2026, 5, 16, 10, 0)

    def _dump_stats(self):
        return None


def executor_for(strategy):
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None
    return executor


class ClosedMarketPreparationTest(unittest.TestCase):
    def setUp(self):
        self.environment = mock.patch.dict(
            os.environ,
            {
                "LUMIBOT_SCHEDULED_EXECUTION": "true",
                "LUMIBOT_SCHEDULED_TARGET_EVENT": "closed_market_prepare",
            },
            clear=False,
        )
        self.market_calendar = mock.patch(
            "lumibot.strategies.strategy_executor.get_trading_days",
            return_value=[{"date": datetime.date(2026, 5, 16)}],
        )
        self.environment.start()
        self.market_calendar.start()

    def tearDown(self):
        self.market_calendar.stop()
        self.environment.stop()

    def test_explicit_closed_market_preparation_runs_without_trading(self):
        strategy = DummyStrategy()

        self.assertTrue(executor_for(strategy).run_once())

        self.assertEqual(strategy.preparation_runs, 1)
        self.assertEqual(strategy.trading_runs, 0)
        self.assertEqual(strategy.end_runs, 0)
        self.assertGreaterEqual(strategy.backups, 1)

    def test_closed_market_without_target_event_keeps_existing_skip_behavior(self):
        os.environ.pop("LUMIBOT_SCHEDULED_TARGET_EVENT", None)
        strategy = DummyStrategy()

        self.assertTrue(executor_for(strategy).run_once())

        self.assertEqual(strategy.preparation_runs, 0)
        self.assertEqual(strategy.trading_runs, 0)
        self.assertEqual(strategy.end_runs, 0)

    def test_closed_market_preparation_blocks_broker_order_mutations(self):
        class Strategy(DummyStrategy):
            def on_closed_market_iteration(self):
                self.broker.submit_order(object())

            def on_bot_crash(self, _error):
                self.broker.submit_order(object())

        strategy = Strategy()
        executor = executor_for(strategy)

        self.assertFalse(executor.run_once())
        self.assertIsInstance(executor.exception, ClosedMarketOrderMutationError)
        self.assertEqual(strategy.trading_runs, 0)
        self.assertEqual(strategy.end_runs, 0)
