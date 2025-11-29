#!/usr/bin/env python3
"""
Test for Buy and Hold strategy with quiet logs functionality
"""

import os
import sys
import io
import pytest
import datetime as dt
import logging
from unittest.mock import patch, MagicMock
import pytz

from lumibot.backtesting import BacktestingBroker
from lumibot.traders import Trader
from lumibot.strategies.strategy import Strategy


class BuyAndHoldQuietLogsTest(Strategy):
    parameters = {
        "buy_symbol": "SPY",
    }

    def initialize(self):
        self.sleeptime = "10M"

    def after_market_closes(self):
        self.log_message("Custom After Market Closes method called")

    def on_trading_iteration(self):
        # Simple buy and hold logic
        buy_symbol = self.parameters["buy_symbol"]
        current_value = 100  # Mock price
        self.log_message(f"Current datetime: {self.get_datetime()}")
        self.log_message(f"The value of {buy_symbol} is {current_value}")
        
        all_positions = self.get_positions()
        if len(all_positions) == 0:
            quantity = int(self.get_portfolio_value() // current_value)
            purchase_order = self.create_order(buy_symbol, quantity, "buy")
            self.submit_order(purchase_order)


@pytest.fixture
def clean_environment():
    """Fixture to save and restore environment"""
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_quiet_logs_buy_and_hold_integration(clean_environment):
    """Integration test for Buy and Hold strategy with quiet logs"""
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"
    os.environ["IS_BACKTESTING"] = "true"  # Set backtesting mode
    
    # Force reload to pick up environment variable
    import importlib
    import lumibot.tools.lumibot_logger
    importlib.reload(lumibot.tools.lumibot_logger)
    
    from lumibot.tools.lumibot_logger import get_strategy_logger
    
    # Test the strategy logger directly
    logger = get_strategy_logger("test", "BuyAndHoldQuietLogsTest")
    
    # Capture log output
    captured_logs = io.StringIO()
    handler = logging.StreamHandler(captured_logs)
    handler.setLevel(logging.DEBUG)
    logger.logger.addHandler(handler)
    
    # Test that INFO messages are suppressed
    logger.info("This INFO message should not appear in quiet mode")
    logger.error("This ERROR message should appear even in quiet mode")
    
    output = captured_logs.getvalue()
    
    # With quiet logs, INFO messages should be suppressed but ERROR should appear
    assert "This INFO message should not appear in quiet mode" not in output
    assert "This ERROR message should appear even in quiet mode" in output