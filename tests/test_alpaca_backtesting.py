from collections import defaultdict
from datetime import datetime, timedelta
import pytz

import pytest
from unittest.mock import MagicMock, patch
import pandas_market_calendars as mcal

from lumibot.backtesting import AlpacaBacktesting, BacktestingBroker
from lumibot.traders import Trader
from lumibot.credentials import ALPACA_CONFIG
from lumibot.strategies import Strategy
from lumibot.entities import Asset

# @pytest.fixture
# def mock_trader():
#     """Mock Trader object."""
#     return MagicMock(spec=Trader)
#
#
# @pytest.fixture
# def mock_broker():
#     """Mock BacktestingBroker object."""
#     return MagicMock(spec=BacktestingBroker)
#
#
# @pytest.fixture
# def alpaca_backtesting(mock_broker, mock_trader):
#     """Fixture for AlpacaBacktesting."""
#     return AlpacaBacktesting(broker=mock_broker, trader=mock_trader)
#


class AlpacaBacktestTestStrategy(Strategy):
    parameters = {"symbol": "AMZN"}

    # Set the initial values for the strategy
    def initialize(self, custom_sleeptime="1D"):
        self.sleeptime = custom_sleeptime
        self.first_price = None
        self.first_option_price = None
        self.orders = []
        self.prices = {}
        self.chains = {}
        self.market_opens_called = False
        self.market_closes_called = False
        # Track times to test LifeCycle order methods. Format: {order_id: {'fill': timestamp, 'submit': timestamp}}
        self.order_time_tracker = defaultdict(lambda: defaultdict(datetime))

    def select_option_expiration(self, chain, days_to_expiration=1):
        """
        Select the option expiration date based on the number of days (from today) until expiration
        :param chain: List of valid option contracts and their expiration dates and strike prices.
            Format: {'TradingClass': 'SPY', 'Multiplier': 100, 'Expirations': [], 'Strikes': []}
        :param days_to_expiration: Number of days until expiration, will select the next expiration date at or after
            this that is available on the exchange
        :return: option expiration as a datetime.date object
        """
        market_cal = mcal.get_calendar("NYSE")  # Typically NYSE, but can be different for some assets
        today = self.get_datetime()
        extra_days_padding = 7  # Some options are not traded every day. Make sure we get enough trading days to check

        # Trading Days DataFrame Format:
        #       index               market_open              market_close
        # =========== ========================= =========================
        #  2012-07-02 2012-07-02 13:30:00+00:00 2012-07-02 20:00:00+00:00
        #  2012-07-03 2012-07-03 13:30:00+00:00 2012-07-03 17:00:00+00:00
        #  2012-07-05 2012-07-05 13:30:00+00:00 2012-07-05 20:00:00+00:00
        trading_days_df = market_cal.schedule(
            start_date=today, end_date=today + timedelta(days=days_to_expiration + extra_days_padding)
        )

        # Look for the next trading day that is in the list of expiration dates. Skip the first trading day because
        # that is today and we want to find the next expiration date.
        #   Date Format: 2023-07-31
        trading_datestrs = [x.to_pydatetime().date() for x in trading_days_df.index.to_list()]
        expirations = self.get_expiration(chain)
        for trading_day in trading_datestrs[days_to_expiration:]:
            day_str = trading_day.strftime("%Y-%m-%d")
            if day_str in expirations:
                return trading_day

        raise ValueError(
            f"Could not find an option expiration date for {days_to_expiration} day(s) " f"from today({today})"
        )

    def before_market_opens(self):
        underlying_asset = Asset(self.parameters["symbol"])
        self.market_opens_called = True
        # self.chains = self.get_chains(underlying_asset)

    def after_market_closes(self):
        orders = self.get_orders()
        self.market_closes_called = True
        self.log_message(f"AlpacaBacktestTestStrategy: {len(orders)} orders executed today")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"AlpacaBacktestTestStrategy: Filled Order: {order}")
        self.order_time_tracker[order.identifier]["fill"] = self.get_datetime()

    def on_new_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: New Order: {order}")
        self.order_time_tracker[order.identifier]["submit"] = self.get_datetime()

    def on_canceled_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: Canceled Order: {order}")
        self.order_time_tracker[order.identifier]["cancel"] = self.get_datetime()

    # Trading Strategy: Backtest will only buy traded assets on first iteration
    def on_trading_iteration(self):
        if self.first_iteration:
            now = self.get_datetime()

            # Create simple option chain | Plugging Amazon "AMZN"; always checking Friday (08/04/23) ensuring
            # Traded_asset exists
            underlying_asset = Asset(self.parameters["symbol"])
            current_asset_price = self.get_last_price(underlying_asset)

            """
            # Option Chain: Get Full Option Chain Information
            chain = self.get_chain(self.chains)
            expiration = self.select_option_expiration(chain, days_to_expiration=1)
            # expiration = datetime.date(2023, 8, 4)

            strike_price = round(current_asset_price)
            option_asset = Asset(
                symbol=underlying_asset.symbol,
                asset_type="option",
                expiration=expiration,
                right="CALL",
                strike=strike_price,
                multiplier=100,
            )
            current_option_price = self.get_last_price(option_asset)
            """

            # Buy 10 shares of the underlying asset for the test
            qty = 10
            self.log_message(f"Buying {qty} shares of {underlying_asset} at {current_asset_price} @ {now}")
            order_underlying_asset = self.create_order(underlying_asset, quantity=qty, side="buy")
            submitted_order = self.submit_order(order_underlying_asset)
            self.orders.append(submitted_order)
            self.prices[submitted_order.identifier] = current_asset_price

            """
            # Buy 1 option contract for the test
            order_option_asset = self.create_order(option_asset, quantity=1, side="buy")
            submitted_order = self.submit_order(order_option_asset)
            self.orders.append(submitted_order)
            self.prices[submitted_order.identifier] = current_option_price

            # Set a stop loss on the underlying asset and cancel it later to test the on_canceled_order() method
            stop_loss_order = self.create_order(
                underlying_asset, quantity=qty, side="sell", stop_price=current_asset_price - 20
            )
            submitted_order = self.submit_order(stop_loss_order)
            self.orders.append(submitted_order)
            """

        # Not the 1st iteration, cancel orders.
        else:
            self.cancel_open_orders()


@pytest.mark.skipif(
    not ALPACA_CONFIG['API_KEY'],
    reason="This test requires an alpaca API key"
)
@pytest.mark.skipif(
    ALPACA_CONFIG['API_KEY'] == '<your key here>',
    reason="This test requires an alpaca API key"
)
class TestAlpacaBacktests:
    """Tests for running backtests with AlpacaBacktesting, BacktestingBroker, and Trader."""

    # @pytest.fixture
    # def setup_backtest(self, mock_trader, mock_broker):
    #     """Setup a mock backtest using AlpacaBacktesting."""
    #     backtesting_instance = AlpacaBacktesting(broker=mock_broker, trader=mock_trader)
    #     return backtesting_instance
    # 
    # def test_backtest_run(self, setup_backtest):
    #     """Test running a backtest with AlpacaBacktesting."""
    #     # Mock running the backtest
    #     setup_backtest.run = MagicMock(return_value="Backtest Run Complete")
    # 
    #     # Call the run method
    #     result = setup_backtest.run()
    # 
    #     # Verify results
    #     assert result == "Backtest Run Complete"
    #     setup_backtest.run.assert_called_once()
    # 
    # def test_broker_trader_integration(self, setup_backtest, mock_broker, mock_trader):
    #     """Test that AlpacaBacktesting integrates with the backtesting broker and trader."""
    #     # Ensure the broker and trader are correctly integrated
    #     assert setup_backtest.broker is mock_broker
    #     assert setup_backtest.trader is mock_trader
    # 
    # def test_backtest_results(self, setup_backtest):
    #     """Test backtest results are properly generated."""
    #     # Mock the results of a backtest
    #     setup_backtest.get_results = MagicMock(return_value={"profit": 1000, "orders": 15})
    # 
    #     # Call the get_results method and check the output
    #     results = setup_backtest.get_results()
    # 
    #     # Verify results match the mocked values
    #     assert results["profit"] == 1000
    #     assert results["orders"] == 15
    #     setup_backtest.get_results.assert_called_once()
    
    def test_1d_data_backtest(self):
        """
        Test AlpacaBacktesting with Lumibot Backtesting and real API calls to Alpaca.
        This test will buy 10 shares of Amazon. 

        TODO: Buy 1 amazon option contract in the historical 2023-08-04 period (in the past!).
        Using the Amazon stock which only has options expiring on Fridays.
        """
        # Parameters: True = Live Trading | False = Backtest
        backtesting_start = datetime(2023, 8, 1)
        backtesting_end = datetime(2023, 8, 4)

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            config=ALPACA_CONFIG
        )
        broker = BacktestingBroker(data_source=data_source)
        strat_obj = AlpacaBacktestTestStrategy(
            broker=broker,
        )
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")
        assert results
        self.verify_backtest_results(strat_obj)

    def test_30m_data_backtest(self):
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime(2024, 2, 7))
        backtesting_end = tzinfo.localize(datetime(2024, 2, 10))

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            config=ALPACA_CONFIG
        )
        broker = BacktestingBroker(data_source=data_source)
        strat_obj = AlpacaBacktestTestStrategy(
            broker=broker,
            custom_sleeptime="30m",
        )

        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")
        assert results
        self.verify_backtest_results(strat_obj)

        # Assert the end datetime is before the market open of the next trading day.
        assert broker.datetime == datetime.fromisoformat("2024-02-12 08:30:00-05:00")

    def verify_backtest_results(self, strat_obj):
        assert isinstance(strat_obj, AlpacaBacktestTestStrategy)

        # Checks bug where LifeCycle methods not being called during PANDAS backtesting
        assert strat_obj.market_opens_called
        assert strat_obj.market_closes_called

        assert len(strat_obj.orders) == 1  # Stock all submitted
        # assert len(strat_obj.orders) == 3  # Stock, option, stoploss all submitted
        # assert len(strat_obj.prices) == 2
        stock_order = strat_obj.orders[0]
        # option_order = strat_obj.orders[1]
        # stoploss_order = strat_obj.orders[2]
        asset_order_id = stock_order.identifier
        # option_order_id = option_order.identifier
        # stoploss_order_id = stoploss_order.identifier
        assert asset_order_id in strat_obj.prices
        # assert option_order_id in strat_obj.prices
        assert 130.0 < strat_obj.prices[asset_order_id] < 140.0, "Valid asset price between 130 and 140"
        sfp = stock_order.get_fill_price()
        assert 130.0 < sfp < 140.0, "Valid Fill price between 130 and 140"
        # assert strat_obj.prices[option_order_id] == 4.10, "Opening Price is $4.10 on 08/01/2023"
        # assert option_order.get_fill_price() == 4.10, "Fills at 1st candle open price of $4.10 on 08/01/2023"
        # assert option_order.is_filled()

        # Check that the on_*_order methods were called
        # Lumibot is autosubmitting 'sell_position' order on cancel to make it 4 total orders
        assert len(strat_obj.order_time_tracker) >= 3
        # Stock order should have been submitted and filled
        assert asset_order_id in strat_obj.order_time_tracker
        assert strat_obj.order_time_tracker[asset_order_id]["submit"]
        assert (
            strat_obj.order_time_tracker[asset_order_id]["fill"]
            >= strat_obj.order_time_tracker[asset_order_id]["submit"]
        )
        # Option order should have been submitted and filled
        # assert option_order_id in strat_obj.order_time_tracker
        # assert strat_obj.order_time_tracker[option_order_id]["submit"]
        # assert (
        #     strat_obj.order_time_tracker[option_order_id]["fill"]
        #     >= strat_obj.order_time_tracker[option_order_id]["submit"]
        # )
        # # Stoploss order should have been submitted and canceled
        # assert stoploss_order_id in strat_obj.order_time_tracker
        # assert strat_obj.order_time_tracker[stoploss_order_id]["submit"]
        # assert (
        #     strat_obj.order_time_tracker[stoploss_order_id]["cancel"]
        #     > strat_obj.order_time_tracker[stoploss_order_id]["submit"]
        # )
        # assert "fill" not in strat_obj.order_time_tracker[stoploss_order_id]


@pytest.mark.skipif(
    not ALPACA_CONFIG['API_KEY'],
    reason="This test requires an alpaca API key"
)
@pytest.mark.skipif(
    ALPACA_CONFIG['API_KEY'] == '<your key here>',
    reason="This test requires an alpaca API key"
)
class TestAlpacaBacktesting:
    """Tests for the AlpacaBacktesting class itself."""

    def test_initialization(self):
        """Test initializing the AlpacaBacktesting class."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        max_memory = 1024

        data_source = AlpacaBacktesting(
            datetime_start=start_date,
            datetime_end=end_date,
            max_memory=max_memory,
            config=ALPACA_CONFIG
        )
        assert data_source.datetime_start == start_date.replace(tzinfo=data_source.datetime_start.tzinfo)
        assert data_source.datetime_end == datetime(2023, 12, 30, 23, 59, tzinfo=data_source.datetime_end.tzinfo)
        assert data_source.MAX_STORAGE_BYTES == max_memory
        assert isinstance(data_source.pandas_data, dict)
