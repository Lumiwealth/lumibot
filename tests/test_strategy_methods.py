from datetime import date, datetime, timedelta
import uuid
from unittest.mock import patch, MagicMock
import pytest

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.entities import Asset, Order, Position
from apscheduler.triggers.cron import CronTrigger
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ


class FakeSnapshotSource:
    def __init__(self):
        self.snapshot = None
        self.last_price_calls = 0

    def get_price_snapshot(self, asset, *args, **kwargs):
        return self.snapshot

    def get_last_price(self, asset, *args, **kwargs):
        self.last_price_calls += 1
        return None


class TestStrategyMethods:
    def test_get_option_expiration_after_date(self):
        """
        Test the get_option_expiration_after_date method by checking that the correct expiration date is returned
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Get the expiration date
        expiry_date = strategy.get_option_expiration_after_date(
            datetime(2023, 4, 2)
        )

        # Check that the expiration date is correct
        assert expiry_date == date(2023, 4, 21)

        # Get the expiration date
        expiry_date = strategy.get_option_expiration_after_date(
            datetime(2023, 7, 12)
        )

        # Check that the expiration date is correct
        assert expiry_date == date(2023, 7, 21)

        # Get the expiration date
        expiry_date = strategy.get_option_expiration_after_date(
            datetime(2023, 6, 29)
        )

        # Check that the expiration date is correct
        assert expiry_date == date(2023, 7, 21)

    def test_validate_order_with_none_quantity(self):
        """
        Test that _validate_order rejects orders with None quantity
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Create an order with None quantity
        order = Order(
            asset=Asset("SPY"), 
            quantity=None, 
            side=Order.OrderSide.BUY, 
            strategy='test_strategy'
        )

        # Test that validation fails
        is_valid = strategy._validate_order(order)
        assert is_valid == False

    def test_validate_order_with_zero_quantity(self):
        """
        Test that _validate_order rejects orders with zero quantity
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Create an order with zero quantity
        order = Order(
            asset=Asset("SPY"), 
            quantity=0, 
            side=Order.OrderSide.BUY, 
            strategy='test_strategy'
        )

        # Test that validation fails
        is_valid = strategy._validate_order(order)
        assert is_valid == False

    def test_validate_order_with_valid_quantity(self):
        """
        Test that _validate_order accepts orders with valid quantity
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Create an order with valid quantity
        order = Order(
            asset=Asset("SPY"), 
            quantity=100, 
            side=Order.OrderSide.BUY, 
            strategy='test_strategy'
        )

        # Test that validation passes
        is_valid = strategy._validate_order(order)
        assert is_valid == True

    def test_validate_order_with_none_order(self):
        """
        Test that _validate_order rejects None orders
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Test that validation fails for None order
        is_valid = strategy._validate_order(None)
        assert is_valid == False

    def test_validate_order_with_invalid_order_type(self):
        """
        Test that _validate_order rejects non-Order objects
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Test that validation fails for non-Order object
        is_valid = strategy._validate_order("not an order")
        assert is_valid == False

    @patch('uuid.uuid4')
    def test_register_cron_callback_returns_job_id(self, mock_uuid4):
        """
        Test that register_cron_callback returns a job ID
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Set is_backtesting to False for this test
        strategy.is_backtesting = False

        # Mock the scheduler's add_job method
        strategy._executor.scheduler.add_job = MagicMock(return_value=None)

        # Define a callback function
        def test_callback():
            pass

        # Register the callback
        job_id = strategy.register_cron_callback("0 9 * * 1-5", test_callback)

        # Check that the job ID is correct
        assert job_id == "cron_callback_test-uuid"

    def test_update_portfolio_value_with_missing_price(self):
        """_update_portfolio_value should skip assets whose prices are missing instead of raising."""

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        asset = Asset("SPY")
        position = Position(strategy._name, asset, quantity=1, avg_fill_price=430.0)
        strategy.broker._filled_positions.append(position)

        with patch.object(strategy.broker.data_source, "get_last_price", return_value=None):
            original_value = strategy.get_portfolio_value()
            updated_value = strategy._update_portfolio_value()

        assert updated_value == original_value

    def _setup_strategy_with_option_position(self):
        date_start = datetime(2024, 1, 1)
        date_end = datetime(2024, 1, 10)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )
        option_asset = Asset(
            "CVNA",
            asset_type="option",
            expiration=date(2026, 1, 16),
            strike=180.0,
            right="CALL",
        )
        option_asset.multiplier = 100
        position = Position(strategy._name, option_asset, quantity=2, avg_fill_price=60.0)
        strategy.broker.get_tracked_positions = MagicMock(return_value=[position])
        strategy._quote_asset = Asset("USD", asset_type="forex")
        source = FakeSnapshotSource()
        strategy.broker.option_source = source
        strategy.broker.data_source = MagicMock()
        strategy.broker.data_source.get_last_price = MagicMock(return_value=None)
        return strategy, position, option_asset, source

    def test_update_portfolio_value_prefers_fresh_trade_snapshot(self):
        strategy, position, option_asset, source = self._setup_strategy_with_option_position()
        now = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 4, 7, 10, 30))
        strategy.broker.data_source.get_datetime = MagicMock(return_value=now)
        source.snapshot = {
            "open": 60.0,
            "high": 66.0,
            "low": 58.0,
            "close": 65.0,
            "bid": 64.5,
            "ask": 65.5,
            "last_trade_time": now - timedelta(seconds=30),
            "last_bid_time": now - timedelta(seconds=20),
            "last_ask_time": now - timedelta(seconds=10),
        }
        starting_cash = strategy.cash

        value = strategy._update_portfolio_value()
        expected_price = 65.0
        assert value == pytest.approx(starting_cash + position.quantity * option_asset.multiplier * expected_price)
        assert source.last_price_calls == 0

    def test_update_portfolio_value_uses_mid_when_trade_stale(self):
        strategy, position, option_asset, source = self._setup_strategy_with_option_position()
        now = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 4, 7, 10, 30))
        strategy.broker.data_source.get_datetime = MagicMock(return_value=now)
        source.snapshot = {
            "open": 60.0,
            "high": 66.0,
            "low": 58.0,
            "close": 65.0,
            "bid": 70.0,
            "ask": 74.0,
            "last_trade_time": now - timedelta(minutes=10),
            "last_bid_time": now - timedelta(seconds=20),
            "last_ask_time": now - timedelta(seconds=10),
        }
        starting_cash = strategy.cash

        with patch.object(strategy.logger, "warning") as warning_mock:
            value = strategy._update_portfolio_value()

        expected_price = (70.0 + 74.0) / 2.0
        assert value == pytest.approx(starting_cash + position.quantity * option_asset.multiplier * expected_price)
        warning_mock.assert_not_called()
        assert source.last_price_calls == 0

    def test_update_portfolio_value_warns_when_all_snapshot_data_stale(self):
        strategy, position, option_asset, source = self._setup_strategy_with_option_position()
        now = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 4, 7, 10, 30))
        strategy.broker.data_source.get_datetime = MagicMock(return_value=now)
        stale_dt = now - timedelta(minutes=10)
        source.snapshot = {
            "open": 60.0,
            "high": 66.0,
            "low": 58.0,
            "close": 65.0,
            "bid": 70.0,
            "ask": 74.0,
            "last_trade_time": stale_dt,
            "last_bid_time": stale_dt,
            "last_ask_time": stale_dt,
        }
        starting_cash = strategy.cash

        with patch.object(strategy.logger, "warning") as warning_mock:
            value = strategy._update_portfolio_value()

        assert value == pytest.approx(starting_cash + position.quantity * option_asset.multiplier * 65.0)
        warning_mock.assert_called_once()
        assert source.last_price_calls == 0

    @patch('uuid.uuid4')
    def test_register_cron_callback_adds_job_to_scheduler(self, mock_uuid4):
        """
        Test that register_cron_callback adds the job to the scheduler with the correct parameters
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Set is_backtesting to False for this test
        strategy.is_backtesting = False

        # Mock the scheduler's add_job method
        strategy._executor.scheduler.add_job = MagicMock(return_value=None)

        # Define a callback function
        def test_callback():
            pass

        # Register the callback
        strategy.register_cron_callback("0 9 * * 1-5", test_callback)

        # Check that add_job was called with the correct parameters
        strategy._executor.scheduler.add_job.assert_called_once()
        args, kwargs = strategy._executor.scheduler.add_job.call_args

        assert args[0] == test_callback
        assert isinstance(args[1], CronTrigger)
        assert kwargs['id'] == "cron_callback_test-uuid"
        assert kwargs['name'] == "Cron Callback: test_callback"
        assert kwargs['jobstore'] == "default"

    @patch('uuid.uuid4')
    def test_register_cron_callback_uses_broker_timezone(self, mock_uuid4):
        """
        Test that register_cron_callback uses the broker's timezone when creating the CronTrigger
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Set is_backtesting to False for this test
        strategy.is_backtesting = False

        # Mock the CronTrigger.from_crontab method
        with patch('apscheduler.triggers.cron.CronTrigger.from_crontab') as mock_from_crontab:
            mock_trigger = MagicMock()
            mock_from_crontab.return_value = mock_trigger

            # Mock the scheduler's add_job method
            strategy._executor.scheduler.add_job = MagicMock(return_value=None)

            # Define a callback function
            def test_callback():
                pass

            # Register the callback
            strategy.register_cron_callback("0 9 * * 1-5", test_callback)

            # Check that from_crontab was called with the broker's timezone
            mock_from_crontab.assert_called_once_with("0 9 * * 1-5", timezone=strategy.pytz)

    @patch('uuid.uuid4')
    def test_register_cron_callback_does_nothing_in_backtesting(self, mock_uuid4):
        """
        Test that register_cron_callback does nothing in backtesting mode
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Ensure is_backtesting is True
        assert strategy.is_backtesting == True

        # Mock the scheduler's add_job method
        strategy._executor.scheduler.add_job = MagicMock(return_value=None)

        # Mock the log_message method to verify it's called
        strategy.log_message = MagicMock()

        # Define a callback function
        def test_callback():
            pass

        # Register the callback
        job_id = strategy.register_cron_callback("0 9 * * 1-5", test_callback)

        # Check that the job ID is correct
        assert job_id == "cron_callback_test-uuid"

        # Check that add_job was not called
        strategy._executor.scheduler.add_job.assert_not_called()

        # Check that log_message was called with the expected message
        strategy.log_message.assert_called_once_with(
            f"Skipping registration of cron callback test_callback in backtesting mode"
        )
