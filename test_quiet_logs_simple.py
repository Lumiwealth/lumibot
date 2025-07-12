#!/usr/bin/env python3

import os
import sys
import logging

# Test the unified logger and quiet logs functionality

# Test 1: Normal logging
print("=== Test 1: Normal logging ===")
from lumibot.tools.lumibot_logger import get_logger, set_log_level

set_log_level("INFO")
logger = get_logger(__name__)
logger.info("This INFO message should show")
logger.warning("This WARNING message should show")
logger.error("This ERROR message should show")

print("\n=== Test 2: Set quiet logs via environment variable ===")
# Test 2: Set BACKTESTING_QUIET_LOGS=true
os.environ['BACKTESTING_QUIET_LOGS'] = 'true'

# Reset the logger module to pick up the new environment variable
import lumibot.tools.lumibot_logger as logger_module
logger_module._handlers_configured = False
logger_module._ensure_handlers_configured()

logger2 = get_logger("test_quiet")
logger2.info("This INFO message should NOT show (quiet logs)")
logger2.warning("This WARNING message should NOT show (quiet logs)")  
logger2.error("This ERROR message SHOULD show (quiet logs)")

print("\n=== Test 3: Test Trader with quiet_logs=True ===")
from lumibot.traders.trader import Trader

# Create a trader with quiet_logs=True
trader = Trader(quiet_logs=True, backtest=True)
trader_logger = get_logger("trader_test")
trader_logger.info("This INFO message should NOT show (trader quiet)")
trader_logger.warning("This WARNING message should NOT show (trader quiet)")
trader_logger.error("This ERROR message SHOULD show (trader quiet)")

print("\n=== Test completed ===")
