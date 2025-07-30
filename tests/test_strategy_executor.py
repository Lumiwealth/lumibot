import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
from apscheduler.triggers.cron import CronTrigger

from lumibot.strategies.strategy_executor import StrategyExecutor


class TestCalculateStrategyTrigger:
    """Test suite for the calculate_strategy_trigger method"""

    def setup_method(self):
        """Set up test fixtures before each test method"""
        # Create a mock strategy
        self.mock_strategy = Mock()
        self.mock_strategy.logger = Mock()

        # Create a mock broker
        self.mock_broker = Mock()
        self.mock_broker.is_market_open.return_value = False
        self.mock_broker.utc_to_local.return_value = datetime(2023, 1, 1, 9, 30, 0)
        self.mock_broker.market_hours.return_value = datetime(2023, 1, 1, 9, 30, 0)

        # Set up the strategy executor
        self.executor = StrategyExecutor(self.mock_strategy)
        self.executor.broker = self.mock_broker

    def test_sleeptime_seconds_single_digit(self):
        """Test that sleeptime in seconds creates correct cron trigger for single digit values"""
        self.mock_strategy.sleeptime = "5S"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that the trigger is set to run every 5 seconds
        assert trigger.fields[5].expressions[0].step == 5  # second field with step 5
        assert self.executor.cron_count_target == 5

    def test_sleeptime_seconds_double_digit(self):
        """Test that sleeptime in seconds creates correct cron trigger for double digit values"""
        self.mock_strategy.sleeptime = "30S"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that the trigger is set to run every 30 seconds
        assert trigger.fields[5].expressions[0].step == 30  # second field with step 30
        assert self.executor.cron_count_target == 30

    def test_sleeptime_seconds_lowercase(self):
        """Test that sleeptime in lowercase 's' works correctly"""
        self.mock_strategy.sleeptime = "15s"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that the trigger is set to run every 15 seconds
        assert trigger.fields[5].expressions[0].step == 15  # second field with step 15
        assert self.executor.cron_count_target == 15

    def test_sleeptime_seconds_large_value(self):
        """Test that sleeptime works with larger second values"""
        self.mock_strategy.sleeptime = "300S"  # 5 minutes in seconds

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that the trigger is set to run every 300 seconds
        assert trigger.fields[5].expressions[0].step == 300  # second field with step 300
        assert self.executor.cron_count_target == 300

    def test_sleeptime_minutes_unchanged(self):
        """Test that sleeptime in minutes still works as before (regression test)"""
        self.mock_strategy.sleeptime = "5M"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that minute field is set to every minute
        assert str(trigger.fields[4]) == "*"  # minute field should be "*"
        assert self.executor.cron_count_target == 5

    def test_sleeptime_integer_unchanged(self):
        """Test that integer sleeptime still works as before (regression test)"""
        self.mock_strategy.sleeptime = 10  # 10 minutes

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that minute field is set to every minute
        assert str(trigger.fields[4]) == "*"  # minute field should be "*"
        assert self.executor.cron_count_target == 10

    def test_sleeptime_hours_unchanged(self):
        """Test that sleeptime in hours still works as before (regression test)"""
        self.mock_strategy.sleeptime = "2H"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that hour field is set to every hour
        assert str(trigger.fields[3]) == "*"  # hour field should be "*"
        assert self.executor.cron_count_target == 2

    def test_sleeptime_days_unchanged(self):
        """Test that sleeptime in days still works as before (regression test)"""
        self.mock_strategy.sleeptime = "1D"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that day field is set to every day
        assert str(trigger.fields[2]) == "*"  # day field should be "*"
        assert self.executor.cron_count_target == 1

    def test_invalid_sleeptime_format(self):
        """Test that invalid sleeptime format raises ValueError"""
        self.mock_strategy.sleeptime = "5X"  # Invalid unit

        with pytest.raises(ValueError) as exc_info:
            self.executor.calculate_strategy_trigger()

        assert "You can set the sleep time as an integer" in str(exc_info.value)

    def test_invalid_sleeptime_type(self):
        """Test that invalid sleeptime type raises ValueError"""
        self.mock_strategy.sleeptime = 5.5  # Float is not supported

        with pytest.raises(ValueError) as exc_info:
            self.executor.calculate_strategy_trigger()

        assert "You can set the sleep time as an integer" in str(exc_info.value)

    def test_force_start_immediately_with_seconds(self):
        """Test that force_start_immediately parameter works with seconds"""
        self.mock_strategy.sleeptime = "10S"

        # Should not affect seconds-based scheduling
        trigger = self.executor.calculate_strategy_trigger(force_start_immediately=True)

        assert isinstance(trigger, CronTrigger)
        assert trigger.fields[5].expressions[0].step == 10  # second field with step 10
        assert self.executor.cron_count_target == 10

    def test_edge_case_one_second(self):
        """Test edge case of 1 second sleeptime"""
        self.mock_strategy.sleeptime = "1S"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that the trigger is set to run every 1 second
        assert trigger.fields[5].expressions[0].step == 1  # second field with step 1
        assert self.executor.cron_count_target == 1

    def test_edge_case_59_seconds(self):
        """Test edge case of 59 seconds sleeptime"""
        self.mock_strategy.sleeptime = "59S"

        trigger = self.executor.calculate_strategy_trigger()

        assert isinstance(trigger, CronTrigger)
        # Check that the trigger is set to run every 59 seconds
        assert trigger.fields[5].expressions[0].step == 59  # second field with step 59
        assert self.executor.cron_count_target == 59

