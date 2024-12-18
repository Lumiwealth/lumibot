import datetime
import os
from collections import defaultdict
from dotenv import load_dotenv
import pandas_market_calendars as mcal
import subprocess
from lumibot.backtesting import BacktestingBroker, ThetaDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.traders import Trader
import psutil
import pytest

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
            start_date=today, end_date=today + datetime.timedelta(days=days_to_expiration + extra_days_padding)
        )
        # Look for the next trading day that is in the list of expiration dates. Skip the first trading day because
        # that is today and we want to find the next expiration date.
        #   Date Format: 2023-07-31
        trading_datestrs = [x.to_pydatetime().date() for x in trading_days_df.index.to_list()]

        for trading_day in trading_datestrs[days_to_expiration:]:
            day_str = trading_day.strftime("%Y-%m-%d")
            if day_str in chain["Expirations"]:
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
        if now.date() == datetime.date(2023, 8, 1) and now.time() == datetime.time(12, 30):
            # Create simple option chain | Plugging Amazon "AMZN"; always checking Friday (08/04/23) ensuring
            # Traded_asset exists
            underlying_asset = Asset(self.parameters["symbol"])
            current_asset_price = self.get_last_price(underlying_asset)

            # Assert that the current asset price is the right price
            assert current_asset_price == 132.18

            # Assert that the current stock quote prices are all correct
            current_ohlcv_bid_ask_quote = self.get_quote(underlying_asset)
            assert current_ohlcv_bid_ask_quote["open"] == 132.18
            assert current_ohlcv_bid_ask_quote["high"] == 132.24
            assert current_ohlcv_bid_ask_quote["low"] == 132.10
            assert current_ohlcv_bid_ask_quote["close"] == 132.10
            assert current_ohlcv_bid_ask_quote["bid"] == 132.10
            assert current_ohlcv_bid_ask_quote["ask"] == 132.12
            assert current_ohlcv_bid_ask_quote["volume"] == 58609
            assert current_ohlcv_bid_ask_quote["bid_size"] == 12
            assert current_ohlcv_bid_ask_quote["bid_condition"] == 1
            assert current_ohlcv_bid_ask_quote["bid_exchange"] == 0
            assert current_ohlcv_bid_ask_quote["ask_size"] == 7
            assert current_ohlcv_bid_ask_quote["ask_condition"] == 60
            assert current_ohlcv_bid_ask_quote["ask_exchange"] == 0

            # Option Chain: Get Full Option Chain Information
            chain = self.get_chain(self.chains, exchange="SMART")
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

            # Get the option price
            current_option_price = self.get_last_price(option_asset)
            # Assert that the current option price is the right price
            assert current_option_price == 4.5

            # Assert that the current option quote prices are all correct
            option_ohlcv_bid_ask_quote = self.get_quote(option_asset)
            assert option_ohlcv_bid_ask_quote["open"] == 4.5
            assert option_ohlcv_bid_ask_quote["high"] == 4.5
            assert option_ohlcv_bid_ask_quote["low"] == 4.5
            assert option_ohlcv_bid_ask_quote["close"] == 4.5
            assert option_ohlcv_bid_ask_quote["bid"] == 4.5
            assert option_ohlcv_bid_ask_quote["ask"] == 4.55
            assert option_ohlcv_bid_ask_quote["volume"] == 5
            assert option_ohlcv_bid_ask_quote["bid_size"] == 5
            assert option_ohlcv_bid_ask_quote["bid_condition"] == 46
            assert option_ohlcv_bid_ask_quote["bid_exchange"] == 50
            assert option_ohlcv_bid_ask_quote["ask_size"] == 1035
            assert option_ohlcv_bid_ask_quote["ask_condition"] == 9
            assert option_ohlcv_bid_ask_quote["ask_exchange"] == 50

            # Get historical prices for the option
            option_prices = self.get_historical_prices(option_asset, 2, "minute")
            df = option_prices.df

            # Assert that the first price is the right price
            assert df["close"].iloc[-1] == 4.6

            # Check that the time of the last bar is 2023-07-31T19:58:00.000Z
            last_dt = df.index[-1]
            assert last_dt == datetime.datetime(2023, 8, 1, 16, 29, tzinfo=datetime.timezone.utc)

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
        assert 130.0 < theta_strat_obj.prices[asset_order_id] < 140.0, "Valid asset price between 130 and 140"
        assert 130.0 < stock_order.get_fill_price() < 140.0, "Valid asset price between 130 and 140"
        assert theta_strat_obj.prices[option_order_id] == 4.5, "Price is $4.5 on 08/01/2023 12:30pm"
        assert option_order.get_fill_price() == 4.5, "Fills at 1st candle open price of $4.10 on 08/01/2023"

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

    # @pytest.mark.skipif(
    #     secrets_not_found,
    #     reason="Skipping test because ThetaData API credentials not found in environment variables",
    # )
    @pytest.mark.skip("Skipping test because ThetaData API credentials not found in Github Pipeline "
                      "environment variables")
    def test_thetadata_restclient(self):
        """
        Test ThetaDataBacktesting with Lumibot Backtesting and real API calls to ThetaData. Using the Amazon stock
        which only has options expiring on Fridays. This test will buy 10 shares of Amazon and 1 option contract
        in the historical 2023-08-04 period (in the past!).
        """
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2023, 8, 1)
        backtesting_end = datetime.datetime(2023, 8, 2)

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
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)

        assert results
        self.verify_backtest_results(strat_obj)


# This will ensure the function runs before any test in this file.
if __name__ == "__main__":
    pytest.main()
