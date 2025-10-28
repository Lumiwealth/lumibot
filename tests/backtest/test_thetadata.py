import datetime
import os
from collections import defaultdict
from datetime import timedelta
from dotenv import load_dotenv
import pandas_market_calendars as mcal
import subprocess
from unittest.mock import MagicMock, patch
from lumibot.backtesting import BacktestingBroker, ThetaDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.traders import Trader
import psutil
import pytest

# Load environment variables from .env file
load_dotenv()

# Define the keyword globally
keyword = 'ThetaTerminal.jar'




def find_git_root(path):
    # Traverse the directories upwards until a .git directory is found
    original_path = path
    while not os.path.isdir(os.path.join(path, '.git')):
        parent_path = os.path.dirname(path)
        if parent_path == path:
            # Reached the root of the filesystem, .git directory not found
            raise Exception(f"No .git directory found starting from {original_path}")
        path = parent_path
    return path


def kill_processes_by_name(keyword):
    try:
        # Find all processes related to the keyword
        result = subprocess.run(['pgrep', '-f', keyword], capture_output=True, text=True)
        pids = result.stdout.strip().split('\n')

        if pids:
            for pid in pids:
                if pid:  # Ensure the PID is not empty
                    print(f"Killing process with PID: {pid}")
                    subprocess.run(['kill', '-9', pid])
            print(f"All processes related to '{keyword}' have been killed.")
        else:
            print(f"No processes found related to '{keyword}'.")

    except Exception as e:
        print(f"An error occurred during kill process: {e}")


@pytest.fixture(scope="module", autouse=True)
def run_before_and_after_tests():
    # Code to execute before running any tests
    kill_processes_by_name(keyword)
    print("Setup before any test")

    yield  # This is where the testing happens

    # Code to execute after all tests
    kill_processes_by_name(keyword)
    print("Teardown after all tests")


try:
    # Find the root of the git repository
    current_dir = os.getcwd()
    git_root = find_git_root(current_dir)
    print(f"The root directory of the Git repository is: {git_root}")
except Exception as e:
    print("ERROR: cannot find the root directory", str(e))

# Global parameters
# Username and Password for ThetaData API
############################################################################################################
# If you are running this test locally, make sure you save the THETADATA_USERNAME and THETADATA_PASSWORD in
# the repo parent directory: lumibot/.secrets/.env file.
THETADATA_USERNAME = os.environ.get("THETADATA_USERNAME")
THETADATA_PASSWORD = os.environ.get("THETADATA_PASSWORD")
############################################################################################################
secrets_not_found = False
if not THETADATA_USERNAME or THETADATA_USERNAME == "uname":
    print("CHECK: Unable to get THETADATA_USERNAME in the environemnt variables.")
    secrets_not_found = True
if not THETADATA_PASSWORD or THETADATA_PASSWORD == "pwd":
    print("CHECK: Unable to get THETADATA_PASSWORD in the environemnt variables.")
    secrets_not_found = True

if secrets_not_found:
    print("ERROR: Unable to get ThetaData API credentials from the environment variables.")


class ThetadataBacktestStrat(Strategy):
    parameters = {"symbol": "AMZN"}

    # Set the initial values for the strategy
    def initialize(self, parameters=None):
        self.sleeptime = "1H"
        self.first_price = None
        self.first_option_price = None
        self.orders = []
        self.prices = {}
        self.chains = {}
        self.market_opens_called = False
        self.market_closes_called = False
        # Track times to test LifeCycle order methods. Format: {order_id: {'fill': timestamp, 'submit': timestamp}}
        self.order_time_tracker = defaultdict(lambda: defaultdict(datetime.datetime))

    def select_option_expiration(self, chains, days_to_expiration=1):
        """
        Select the option expiration date based on the number of days (from today) until expiration
        :param chains: Chains object with option contracts.
            Uses chains.expirations() method to get list of available expiration dates
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
            start_date=today, end_date=today + datetime.timedelta(days=days_to_expiration + extra_days_padding)
        )
        # Look for the next trading day that is in the list of expiration dates. Skip the first trading day because
        # that is today and we want to find the next expiration date.
        #   Date Format: 2023-07-31
        trading_datestrs = [x.to_pydatetime().date() for x in trading_days_df.index.to_list()]

        # Get available expirations from the Chains object (modern API)
        available_expirations = chains.expirations("CALL")  # Use CALL side arbitrarily

        for trading_day in trading_datestrs[days_to_expiration:]:
            day_str = trading_day.strftime("%Y-%m-%d")
            if day_str in available_expirations:
                return trading_day

        raise ValueError(
            f"Could not find an option expiration date for {days_to_expiration} day(s) " f"from today({today})"
        )

    def before_market_opens(self):
        underlying_asset = Asset(self.parameters["symbol"])
        self.market_opens_called = True
        self.chains = self.get_chains(underlying_asset)

    def after_market_closes(self):
        orders = self.get_orders()
        self.market_closes_called = True
        self.log_message(f"ThetadataBacktestStrat: {len(orders)} orders executed today")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"ThetadataBacktestStrat: Filled Order: {order}")
        self.order_time_tracker[order.identifier]["fill"] = self.get_datetime()

    def on_new_order(self, order):
        self.log_message(f"ThetadataBacktestStrat: New Order: {order}")
        self.order_time_tracker[order.identifier]["submit"] = self.get_datetime()

    def on_canceled_order(self, order):
        self.log_message(f"ThetadataBacktestStrat: Canceled Order: {order}")
        self.order_time_tracker[order.identifier]["cancel"] = self.get_datetime()

    # Trading Strategy: Backtest will only buy traded assets on first iteration
    def on_trading_iteration(self):
        # if self.first_iteration:
        now = self.get_datetime()
        if now.date() == datetime.date(2024, 8, 1) and now.time() == datetime.time(12, 30):
            # Create simple option chain | Plugging Amazon "AMZN"; always checking Friday (08/02/24) ensuring
            # Traded_asset exists
            underlying_asset = Asset(self.parameters["symbol"])
            current_asset_price = self.get_last_price(underlying_asset)

            # Assert that the current asset price is in reasonable range (prices change over time)
            assert 150 < current_asset_price < 200, f"AMZN price should be between $150-200, got {current_asset_price}"

            # Assert that we can get a quote for the asset
            current_ohlcv_bid_ask_quote = self.get_quote(underlying_asset)
            assert current_ohlcv_bid_ask_quote is not None
            assert current_ohlcv_bid_ask_quote.price is not None and current_ohlcv_bid_ask_quote.price > 0
            # Check volume if available
            if current_ohlcv_bid_ask_quote.volume:
                assert current_ohlcv_bid_ask_quote.volume > 0

            # Option Chain: Get Full Option Chain Information (Chains object now)
            expiration = self.select_option_expiration(self.chains, days_to_expiration=1)

            strike_price = round(current_asset_price)
            option_asset = Asset(
                symbol=underlying_asset.symbol,
                asset_type="option",
                expiration=expiration,
                right="CALL",
                strike=strike_price,
                multiplier=100,
            )

            # Get the option price
            current_option_price = self.get_last_price(option_asset)
            # Assert that the current option price is reasonable (> 0)
            assert current_option_price > 0, f"Option price should be positive, got {current_option_price}"

            # Assert that we can get a quote for the option
            option_ohlcv_bid_ask_quote = self.get_quote(option_asset)
            assert option_ohlcv_bid_ask_quote is not None
            assert option_ohlcv_bid_ask_quote.price is not None and option_ohlcv_bid_ask_quote.price > 0

            # Get historical prices for the option
            option_prices = self.get_historical_prices(option_asset, 2, "minute")
            df = option_prices.df

            # Assert that we got historical data
            assert len(df) > 0
            assert df["close"].iloc[-1] > 0

            # Check that the time of the last bar is on the correct date
            last_dt = df.index[-1]
            assert last_dt.date() == datetime.date(2024, 8, 1)

            # Buy 10 shares of the underlying asset for the test
            qty = 10
            self.log_message(f"Buying {qty} shares of {underlying_asset} at {current_asset_price} @ {now}")
            order_underlying_asset = self.create_order(underlying_asset, quantity=qty, side="buy")
            submitted_order = self.submit_order(order_underlying_asset)
            self.orders.append(submitted_order)
            self.prices[submitted_order.identifier] = current_asset_price

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

        # # Not the 1st iteration, cancel orders.
        else:
            self.cancel_open_orders()


class TestThetaDataBacktestFull:
    def verify_backtest_results(self, theta_strat_obj):
        assert isinstance(theta_strat_obj, ThetadataBacktestStrat)

        # Checks bug where LifeCycle methods not being called during PANDAS backtesting
        assert theta_strat_obj.market_opens_called
        assert theta_strat_obj.market_closes_called

        assert len(theta_strat_obj.orders) == 3  # Stock, option, stoploss all submitted
        assert len(theta_strat_obj.prices) == 2
        stock_order = theta_strat_obj.orders[0]
        option_order = theta_strat_obj.orders[1]
        stoploss_order = theta_strat_obj.orders[2]
        asset_order_id = stock_order.identifier
        option_order_id = option_order.identifier
        stoploss_order_id = stoploss_order.identifier
        assert asset_order_id in theta_strat_obj.prices
        assert option_order_id in theta_strat_obj.prices
        assert 150.0 < theta_strat_obj.prices[asset_order_id] < 200.0, "Valid AMZN price between 150 and 200"
        assert 150.0 < stock_order.get_fill_price() < 200.0, "Valid AMZN price between 150 and 200"
        assert theta_strat_obj.prices[option_order_id] > 0, "Option price should be positive"
        assert option_order.get_fill_price() > 0, "Option fill price should be positive"

        assert option_order.is_filled()

        # Check that the on_*_order methods were called
        # Lumibot is autosubmitting 'sell_position' order on cancel to make it 4 total orders
        assert len(theta_strat_obj.order_time_tracker) >= 3
        # Stock order should have been submitted and filled
        assert asset_order_id in theta_strat_obj.order_time_tracker
        assert theta_strat_obj.order_time_tracker[asset_order_id]["submit"]
        assert (
            theta_strat_obj.order_time_tracker[asset_order_id]["fill"]
            >= theta_strat_obj.order_time_tracker[asset_order_id]["submit"]
        )
        # Option order should have been submitted and filled
        assert option_order_id in theta_strat_obj.order_time_tracker
        assert theta_strat_obj.order_time_tracker[option_order_id]["submit"]
        assert (
            theta_strat_obj.order_time_tracker[option_order_id]["fill"]
            >= theta_strat_obj.order_time_tracker[option_order_id]["submit"]
        )
        # Stoploss order should have been submitted and canceled
        assert stoploss_order_id in theta_strat_obj.order_time_tracker
        assert theta_strat_obj.order_time_tracker[stoploss_order_id]["submit"]
        assert (
            theta_strat_obj.order_time_tracker[stoploss_order_id]["cancel"]
            > theta_strat_obj.order_time_tracker[stoploss_order_id]["submit"]
        )
        assert "fill" not in theta_strat_obj.order_time_tracker[stoploss_order_id]

    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    def test_thetadata_restclient(self):
        """
        Test ThetaDataBacktesting with Lumibot Backtesting and real API calls to ThetaData. Using the Amazon stock
        which only has options expiring on Fridays. This test will buy 10 shares of Amazon and 1 option contract
        in the historical 2024-08-01 period (in the past!).
        """
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2024, 8, 1)
        backtesting_end = datetime.datetime(2024, 8, 2)

        data_source = ThetaDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            username=THETADATA_USERNAME,
            password=THETADATA_PASSWORD,
        )
        broker = BacktestingBroker(data_source=data_source)
        strat_obj = ThetadataBacktestStrat(
            broker=broker,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
        )
        trader = Trader(backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

        assert results
        self.verify_backtest_results(strat_obj)

    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    def test_intraday_daterange(self):
        """Test intraday date range bar counts"""
        import pytz
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 8, 1, 9, 30))
        end = tzinfo.localize(datetime.datetime(2024, 8, 1, 16, 0))

        data_source = ThetaDataBacktesting(
            start, end, username=THETADATA_USERNAME, password=THETADATA_PASSWORD
        )

        # Get minute bars for full trading day
        asset = Asset(symbol="SPY", asset_type="stock")
        data_source._datetime = tzinfo.localize(datetime.datetime(2024, 8, 1, 15, 0))
        prices = data_source.get_historical_prices(asset, 400, "minute")

        assert prices is not None
        assert len(prices.df) > 0
        # Full trading day should have ~390 bars
        assert 350 <= len(prices.df) <= 400


class TestThetaDataSource:
    """Additional tests for ThetaData data source functionality"""

    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    def test_get_historical_prices(self):
        """Test get_historical_prices for various scenarios"""
        import pytz
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 8, 1))
        end = tzinfo.localize(datetime.datetime(2024, 8, 5))

        data_source = ThetaDataBacktesting(
            start, end, username=THETADATA_USERNAME, password=THETADATA_PASSWORD
        )
        data_source._datetime = tzinfo.localize(datetime.datetime(2024, 8, 5, 10))

        # Test minute bars
        prices = data_source.get_historical_prices("SPY", 2, "minute")
        assert prices is not None
        assert len(prices.df) > 0

        # Test day bars
        day_prices = data_source.get_historical_prices("SPY", 5, "day")
        assert day_prices is not None
        assert len(day_prices.df) > 0

    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    def test_get_chains_spy_expected_data(self):
        """Test options chain retrieval for SPY"""
        import pytz
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 8, 1))
        end = tzinfo.localize(datetime.datetime(2024, 8, 5))

        data_source = ThetaDataBacktesting(
            start, end, username=THETADATA_USERNAME, password=THETADATA_PASSWORD
        )

        asset = Asset(symbol="SPY", asset_type="stock")
        chains = data_source.get_chains(asset)

        assert chains is not None
        # Check for expiration dates
        expirations = chains.expirations("CALL")
        assert len(expirations) > 0

        # Check for strike prices
        first_exp = expirations[0]
        strikes = chains.strikes(first_exp, "CALL")
        assert len(strikes) > 10
        assert min(strikes) > 300
        assert max(strikes) < 700

    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    def test_get_last_price_unchanged(self):
        """Verify price caching works"""
        import pytz
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 8, 1))
        end = tzinfo.localize(datetime.datetime(2024, 8, 5))

        data_source = ThetaDataBacktesting(
            start, end, username=THETADATA_USERNAME, password=THETADATA_PASSWORD
        )

        asset = Asset(symbol="AAPL", asset_type="stock")

        # Get price twice - should be cached
        price1 = data_source.get_last_price(asset)
        price2 = data_source.get_last_price(asset)

        assert price1 == price2
        assert price1 > 0

    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    def test_get_historical_prices_unchanged_for_amzn(self):
        """Reproducibility test - same parameters should give same results"""
        import pytz
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 8, 1))
        end = tzinfo.localize(datetime.datetime(2024, 8, 5))

        data_source1 = ThetaDataBacktesting(
            start, end, username=THETADATA_USERNAME, password=THETADATA_PASSWORD
        )
        data_source2 = ThetaDataBacktesting(
            start, end, username=THETADATA_USERNAME, password=THETADATA_PASSWORD
        )

        asset = Asset(symbol="AMZN", asset_type="stock")

        # Get historical prices from both
        prices1 = data_source1.get_historical_prices(asset, 5, "day")
        prices2 = data_source2.get_historical_prices(asset, 5, "day")

        assert len(prices1.df) == len(prices2.df)
        # Prices should be identical
        assert (prices1.df['close'].values == prices2.df['close'].values).all()

    @pytest.mark.apitest
    @pytest.mark.skipif(
        secrets_not_found,
        reason="Skipping test because ThetaData API credentials not found in environment variables",
    )
    def test_pull_source_symbol_bars_with_api_call(self, mocker):
        """Test that thetadata_helper.get_price_data() is called with correct parameters"""
        import pytz
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 8, 1))
        end = tzinfo.localize(datetime.datetime(2024, 8, 5))

        data_source = ThetaDataBacktesting(
            start, end, username=THETADATA_USERNAME, password=THETADATA_PASSWORD
        )

        # Mock the datetime to first date
        mocker.patch.object(
            data_source,
            'get_datetime',
            return_value=data_source.datetime_start
        )

        # Mock the helper function
        mocked_get_price_data = mocker.patch(
            'lumibot.tools.thetadata_helper.get_price_data',
            return_value=MagicMock()
        )

        asset = Asset(symbol="AAPL", asset_type="stock")
        quote = Asset(symbol="USD", asset_type="forex")
        length = 10
        timestep = "day"
        START_BUFFER = timedelta(days=5)

        with patch('lumibot.backtesting.thetadata_backtesting.START_BUFFER', new=START_BUFFER):
            data_source._pull_source_symbol_bars(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote
            )

            # Verify the function was called with expected parameters
            assert mocked_get_price_data.called
            call_args = mocked_get_price_data.call_args

            # Check that the asset was passed in the call (either as positional or keyword arg)
            # The function signature may have username as first parameter
            assert asset in call_args[0] or call_args[1].get('asset') == asset, \
                f"Asset {asset} not found in call args: {call_args}"


# This will ensure the function runs before any test in this file.
if __name__ == "__main__":
    pytest.main()
