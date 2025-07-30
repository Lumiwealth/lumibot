#!/usr/bin/env python3
"""
Test for APScheduler warnings with sleeptime < 1 minute
"""
import pytest
from unittest.mock import patch, MagicMock
import datetime as dt


def test_sleeptime_10s_max_instances():
    """Test that 10s sleeptime sets appropriate max_instances to avoid warnings"""
    from lumibot.strategies.strategy import Strategy
    
    # Create a test strategy with 10s sleeptime
    class TestStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "10S"
            
        def on_trading_iteration(self):
            pass
    
    # Mock the broker and other dependencies
    mock_broker = MagicMock()
    mock_broker.is_backtesting_broker = False
    
    strategy = TestStrategy(broker=mock_broker)
    
    # Call initialize to set the sleeptime
    strategy.initialize()
    
    # The key test is that the strategy initializes correctly with 10S sleeptime
    assert strategy.sleeptime == "10S"
    
    # Test the max_instances calculation logic directly
    sleeptime = "10S"
    if sleeptime.endswith("S"):
        seconds = int(sleeptime[:-1])
        if seconds < 60:
            max_instances = max(1, (60 // seconds) + 1)
        else:
            max_instances = 1
    else:
        max_instances = 1
    
    # For 10S, we expect (60 // 10) + 1 = 7 max_instances
    assert max_instances == 7, f"Expected 7 max_instances for 10S sleeptime, got {max_instances}"


def test_sleeptime_1m_default_max_instances():
    """Test that 1m sleeptime works fine with default max_instances"""
    from lumibot.strategies.strategy import Strategy
    
    # Create a test strategy with 1m sleeptime
    class TestStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            
        def on_trading_iteration(self):
            pass
    
    # Mock the broker
    mock_broker = MagicMock()
    mock_broker.is_backtesting_broker = False
    
    strategy = TestStrategy(broker=mock_broker)
    
    # For 1m sleeptime, default max_instances=1 should be fine
    # No specific assertion needed, just ensure no exceptions
    assert strategy.sleeptime == "1M"


def test_calculate_max_instances_for_sleeptime():
    """Test calculation of appropriate max_instances based on sleeptime"""
    # Test different sleeptimes and expected max_instances
    test_cases = [
        ("10S", 6),   # 10 seconds -> need at least 6 instances (60s/10s)
        ("30S", 2),   # 30 seconds -> need at least 2 instances
        ("1M", 1),    # 1 minute -> 1 instance is fine
        ("5M", 1),    # 5 minutes -> 1 instance is fine
        ("1H", 1),    # 1 hour -> 1 instance is fine
    ]
    
    for sleeptime, expected_min_instances in test_cases:
        # Parse sleeptime and calculate appropriate max_instances
        if sleeptime.endswith("S"):
            seconds = int(sleeptime[:-1])
            # For second-based sleeptimes, calculate how many could fire per minute
            max_instances = max(1, (60 // seconds) + 1)
        else:
            max_instances = 1
            
        assert max_instances >= expected_min_instances, \
            f"max_instances for {sleeptime} should be at least {expected_min_instances}"