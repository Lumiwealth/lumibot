import unittest
from unittest.mock import MagicMock, patch

from lumibot.strategies import strategy_executor as executor_module
from lumibot.strategies.strategy_executor import StrategyExecutor


def _make_executor(crashes=2, max_iterations=10):
    """Executor with a mock live broker whose trading session crashes `crashes` times."""
    broker = MagicMock()
    broker.IS_BACKTESTING_BROKER = False
    broker.market = "NYSE"
    calls = {"n": 0}

    def should_continue():
        calls["n"] += 1
        return calls["n"] <= max_iterations

    broker.should_continue.side_effect = should_continue

    strategy = MagicMock()
    strategy.is_backtesting = False
    strategy._analysis = {}
    strategy.broker = broker

    executor = StrategyExecutor(strategy)
    executor._initialize = MagicMock()
    executor._on_bot_crash = MagicMock()
    executor._advance_to_next_trading_day = MagicMock(return_value=True)
    executor._on_strategy_end = MagicMock()
    executor._run_trading_session = MagicMock(
        side_effect=[RuntimeError("simulated network crash")] * crashes + [None] * max_iterations
    )
    return executor


class TestLiveCrashBackoff(unittest.TestCase):
    def test_repeated_session_crash_backs_off_before_retry(self):
        """https://github.com/Lumiwealth/lumibot/issues/1145

        When the live trading session keeps crashing (e.g. network outage),
        the executor must pause briefly before retrying instead of hot-spinning
        the run loop at 100% CPU.
        """
        executor = _make_executor(crashes=2)

        with patch("time.sleep") as mock_sleep, patch.object(
            executor_module, "get_trading_days", return_value=[]
        ):
            result = executor.run()

        self.assertTrue(result)
        self.assertEqual(executor._on_bot_crash.call_count, 2)
        self.assertGreaterEqual(
            mock_sleep.call_count,
            2,
            "expected a backoff sleep after every crashed trading session (issue #1145)",
        )


if __name__ == "__main__":
    unittest.main()
