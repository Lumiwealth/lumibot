from datetime import date, datetime

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.entities import Asset, Order


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
