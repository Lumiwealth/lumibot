from datetime import date, datetime
import uuid
from unittest.mock import patch, MagicMock

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.entities import Asset, Order
from apscheduler.triggers.cron import CronTrigger


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
