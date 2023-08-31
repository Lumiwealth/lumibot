from datetime import date, datetime

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold

date_start = datetime(2021, 7, 10)
date_end = datetime(2021, 7, 13)
data_source = YahooDataBacktesting(date_start, date_end)
backtesting_broker = BacktestingBroker(data_source)
strategy = BuyAndHold(
    backtesting_broker,
)


class TestStrategyMethods:
    def test_get_option_expiration_after_date(self):
        """
        Test the get_option_expiration_after_date method by checking that the correct expiration date is returned
        """

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
