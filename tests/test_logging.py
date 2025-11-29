import datetime
import logging
import pytest

from lumibot.example_strategies.lifecycle_logger import LifecycleLogger
from lumibot.backtesting import YahooDataBacktesting


class TestLogging:

    def test_logging(self, caplog):
        caplog.set_level(logging.INFO)
        logger = logging.getLogger()
        logger.info("This is an info message")
        assert "This is an info message" in caplog.text

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_backtest_produces_no_logs_when_quiet_logs_is_true(self, caplog):
        caplog.set_level(logging.INFO)
        backtesting_start = datetime.datetime(2023, 1, 2)
        backtesting_end = datetime.datetime(2023, 1, 4)

        LifecycleLogger.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            parameters={"sleeptime": "1D", "market": "NYSE"},
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=True,
            quiet_logs=True,
        )
        # count that this contains 3 new lines. Its an easy proxy for the number of log messages and avoids
        # the issue where the datetime is always gonna be different.
        assert caplog.text.count("\n") == 4
        assert "Starting backtest...\n" in caplog.text
        assert "Backtesting starting...\n" in caplog.text
        assert "Backtesting finished\n" in caplog.text
        assert "Backtest took " in caplog.text

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_backtest_produces_logs_when_quiet_logs_is_false(self, caplog):
        caplog.set_level(logging.INFO)
        backtesting_start = datetime.datetime(2023, 1, 2)
        backtesting_end = datetime.datetime(2023, 1, 4)

        LifecycleLogger.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            parameters={"sleeptime": "1D", "market": "NYSE"},
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=False,
        )

        assert caplog.text.count("\n") >= 9
        assert "Starting backtest...\n" in caplog.text
        assert "Backtesting starting...\n" in caplog.text
        assert "before_market_opens called\n" in caplog.text
        assert "before_starting_trading called\n" in caplog.text
        assert "on_trading_iteration called\n" in caplog.text
        assert "before_market_closes called\n" in caplog.text
        assert "after_market_closes called\n" in caplog.text
        assert "Backtesting finished\n" in caplog.text
