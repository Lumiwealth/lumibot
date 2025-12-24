import datetime
import os
from collections import defaultdict

import numpy as np
import pandas as pd
import pytest
import pandas_market_calendars as mcal
from pandas.testing import assert_frame_equal
from dotenv import load_dotenv

from tests.fixtures import polygon_data_backtesting
import pytz
from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.traders import Trader

from unittest.mock import MagicMock, patch
from datetime import timedelta

# Load environment variables from .env file
load_dotenv()

# Global parameters
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")


# LEGACY TEST CLASS (created Aug 2023)
# These tests explicitly test PolygonDataBacktesting and must not be overridden.
@pytest.mark.usefixtures("disable_datasource_override")
class PolygonBacktestStrat(Strategy):
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
        self.chains = self.get_chains(underlying_asset)

    def after_market_closes(self):
        orders = self.get_orders()
        self.market_closes_called = True
        self.log_message(f"PolygonBacktestStrat: {len(orders)} orders executed today")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"PolygonBacktestStrat: Filled Order: {order}")
        self.order_time_tracker[order.identifier]["fill"] = self.get_datetime()

    def on_new_order(self, order):
        self.log_message(f"PolygonBacktestStrat: New Order: {order}")
        self.order_time_tracker[order.identifier]["submit"] = self.get_datetime()

    def on_canceled_order(self, order):
        self.log_message(f"PolygonBacktestStrat: Canceled Order: {order}")
        self.order_time_tracker[order.identifier]["cancel"] = self.get_datetime()

    # Trading Strategy: Backtest will only buy traded assets on first iteration
    def on_trading_iteration(self):
        if self.first_iteration:
            now = self.get_datetime()

            # Create simple option chain | Plugging Amazon "AMZN"; always checking Friday (08/04/23) ensuring
            # Traded_asset exists
            underlying_asset = Asset(self.parameters["symbol"])
            current_asset_price = self.get_last_price(underlying_asset)

            # Option Chain: Get Full Option Chain Information
            chain = self.get_chain(self.chains)
            expiration = self.select_option_expiration(chain, days_to_expiration=1)
            # expiration = datetime.date(2023, 8, 4)

            # Get the stike price closest to the current asset price
            expiration_str = expiration.strftime("%Y-%m-%d")
            strike_price = min(chain['Chains']['CALL'][expiration_str], key=lambda x: abs(x - current_asset_price))

            option_asset = Asset(
                symbol=underlying_asset.symbol,
                asset_type="option",
                expiration=expiration,
                right="CALL",
                strike=strike_price,
                multiplier=100,
            )
            current_option_price = self.get_last_price(option_asset)

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

        # Not the 1st iteration, cancel orders.
        else:
            self.cancel_open_orders()


class TestPolygonBacktestFull:

    def verify_backtest_results(self, poly_strat_obj):
        assert isinstance(poly_strat_obj, PolygonBacktestStrat)

        # Checks bug where LifeCycle methods not being called during PANDAS backtesting
        assert poly_strat_obj.market_opens_called
        assert poly_strat_obj.market_closes_called

        assert len(poly_strat_obj.orders) == 3  # Stock, option, stoploss all submitted
        assert len(poly_strat_obj.prices) == 2
        stock_order = poly_strat_obj.orders[0]
        option_order = poly_strat_obj.orders[1]
        stoploss_order = poly_strat_obj.orders[2]
        asset_order_id = stock_order.identifier
        option_order_id = option_order.identifier
        stoploss_order_id = stoploss_order.identifier
        assert asset_order_id in poly_strat_obj.prices
        assert option_order_id in poly_strat_obj.prices
        assert 185.0 < poly_strat_obj.prices[asset_order_id] < 195.0, "Valid asset price between 185 and 195"
        sfp = stock_order.get_fill_price()
        assert 185.0 < sfp < 195.0, "Valid Fill price between 185 and 195"
        assert poly_strat_obj.prices[option_order_id] == 6.10, "Opening Price is $6.10 on 08/01/2023"
        assert option_order.get_fill_price() == 6.10, "Fills at 1st candle open price of $6.10 on 08/01/2023"

        assert option_order.is_filled()

        # Check that the on_*_order methods were called
        # Lumibot is autosubmitting 'sell_position' order on cancel to make it 4 total orders
        assert len(poly_strat_obj.order_time_tracker) >= 3
        # Stock order should have been submitted and filled
        assert asset_order_id in poly_strat_obj.order_time_tracker
        assert poly_strat_obj.order_time_tracker[asset_order_id]["submit"]
        assert (
            poly_strat_obj.order_time_tracker[asset_order_id]["fill"]
            >= poly_strat_obj.order_time_tracker[asset_order_id]["submit"]
        )
        # Option order should have been submitted and filled
        assert option_order_id in poly_strat_obj.order_time_tracker
        assert poly_strat_obj.order_time_tracker[option_order_id]["submit"]
        assert (
            poly_strat_obj.order_time_tracker[option_order_id]["fill"]
            >= poly_strat_obj.order_time_tracker[option_order_id]["submit"]
        )
        # Stoploss order should have been submitted and either canceled or filled
        # (depending on market conditions, the stop may trigger before cancel_open_orders is called)
        assert stoploss_order_id in poly_strat_obj.order_time_tracker
        assert poly_strat_obj.order_time_tracker[stoploss_order_id]["submit"]

        # Check if it was canceled or filled
        if "cancel" in poly_strat_obj.order_time_tracker[stoploss_order_id]:
            # Order was canceled before it could fill
            assert (
                poly_strat_obj.order_time_tracker[stoploss_order_id]["cancel"]
                > poly_strat_obj.order_time_tracker[stoploss_order_id]["submit"]
            )
            assert "fill" not in poly_strat_obj.order_time_tracker[stoploss_order_id]
        elif "fill" in poly_strat_obj.order_time_tracker[stoploss_order_id]:
            # Order filled before it could be canceled (stop price was hit)
            assert (
                poly_strat_obj.order_time_tracker[stoploss_order_id]["fill"]
                > poly_strat_obj.order_time_tracker[stoploss_order_id]["submit"]
            )
        else:
            # Order should have been either canceled or filled
            assert False, f"Stoploss order {stoploss_order_id} was neither canceled nor filled"

    @pytest.mark.skipif(
        not POLYGON_API_KEY,
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_API_KEY == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_polygon_restclient(self):
        """
        Test Polygon REST Client with Lumibot Backtesting and real API calls to Polygon. Using the Amazon stock
        which only has options expiring on Fridays. This test will buy 10 shares of Amazon and 1 option contract
        in the historical 2023-08-04 period (in the past!).
        """
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2024, 8, 1)
        # Ensure the entire last day is processed by setting time to end of day
        backtesting_end = datetime.datetime(2024, 8, 4, 23, 59, 59)

        data_source = PolygonDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=POLYGON_API_KEY,
        )
        broker = BacktestingBroker(data_source=data_source)
        poly_strat_obj = PolygonBacktestStrat(
            broker=broker,
        )
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(poly_strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

        assert results
        self.verify_backtest_results(poly_strat_obj)

    @pytest.mark.skipif(
        not POLYGON_API_KEY,
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_API_KEY == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_intraday_daterange(self, disable_datasource_override):
        tzinfo = pytz.timezone("America/New_York")
        # Keep the window small to avoid hitting Polygon rate limits in CI while still
        # validating intraday date range handling (end before next session opens).
        backtesting_start = tzinfo.localize(datetime.datetime(2024, 2, 7, 8, 0, 0))
        backtesting_end = tzinfo.localize(datetime.datetime(2024, 2, 8, 8, 30, 0))

        data_source = PolygonDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=POLYGON_API_KEY,
        )
        broker = BacktestingBroker(data_source=data_source)

        class IntradayDateRangeStrat(Strategy):
            def initialize(self):
                self.sleeptime = "30m"

            def on_trading_iteration(self):
                return

        poly_strat_obj = IntradayDateRangeStrat(broker=broker)
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(poly_strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False, tearsheet_file="")
        # Assert the results are not empty
        assert results
        # Assert the end datetime is before the market open of the next trading day.
        assert broker.datetime == datetime.datetime.fromisoformat("2024-02-08 08:30:00-05:00")

    @pytest.mark.skipif(
        not POLYGON_API_KEY,
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_API_KEY == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_polygon_legacy_backtest(self):
        """
        Do the same backtest as test_polygon_restclient() but using the legacy backtest() function call instead of
        trader.run_all(backtest=True) (which is the new standard way to run backtests).
        """

        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2024, 8, 1)
        backtesting_end = datetime.datetime(2024, 8, 4)

        # Execute Backtest | Polygon.io API Connection
        results, poly_strat_obj = PolygonBacktestStrat.run_backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            minutes_before_opening=5,
            minutes_before_closing=5,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        self.verify_backtest_results(poly_strat_obj)

    @pytest.mark.skipif(
        not POLYGON_API_KEY,
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_API_KEY == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_polygon_legacy_backtest2(self):
        """Test that the legacy backtest() function call works without returning the startegy object"""
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2024, 8, 1)
        backtesting_end = datetime.datetime(2024, 8, 4)

        # Execute Backtest | Polygon.io API Connection
        results = PolygonBacktestStrat.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            polygon_api_key=POLYGON_API_KEY,  # Testing the legacy parameter name while DeprecationWarning is active
        )
        assert results

    def test_pull_source_symbol_bars_with_api_call(self, polygon_data_backtesting, mocker):
        """Test that polygon_helper.get_price_data_from_polygon() is called with the right parameters"""
        # Only simulate first date
        mocker.patch.object(
            polygon_data_backtesting,
            'get_datetime',
            return_value=polygon_data_backtesting.datetime_start
        )

        mocked_get_price_data = mocker.patch(
            'lumibot.tools.polygon_helper.get_price_data_from_polygon',
            return_value=MagicMock()
        )

        asset = Asset(symbol="AAPL", asset_type="stock")
        quote = Asset(symbol="USD", asset_type="forex")
        length = 10
        timestep = "day"
        START_BUFFER = timedelta(days=5)

        with patch('lumibot.backtesting.polygon_backtesting.START_BUFFER', new=START_BUFFER):
            polygon_data_backtesting._pull_source_symbol_bars(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote
            )

            mocked_get_price_data.assert_called_once()
            call_args = mocked_get_price_data.call_args

            extra_padding_days = (length // 5) * 3
            expected_start_date = polygon_data_backtesting.datetime_start - \
                datetime.timedelta(days=length + extra_padding_days) - START_BUFFER

            assert call_args[0][0] == polygon_data_backtesting._api_key
            assert call_args[0][1] == asset
            assert call_args[0][2] == expected_start_date
            assert call_args[0][3] == polygon_data_backtesting.datetime_end
            assert call_args[1]["timespan"] == timestep
            assert call_args[1]["quote_asset"] == quote


class TestPolygonDataSource:

    @pytest.mark.skipif(
        not POLYGON_API_KEY,
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_API_KEY == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_get_historical_prices(self):
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 2, 5))
        end = tzinfo.localize(datetime.datetime(2024, 2, 10))

        data_source = PolygonDataBacktesting(
            start, end, api_key=POLYGON_API_KEY
        )
        data_source._datetime = tzinfo.localize(datetime.datetime(2024, 2, 7, 10))
        # This call will set make the data source use minute bars.
        prices = data_source.get_historical_prices("SPY", 2, "minute")
        # The data source will aggregate day bars from the minute bars.
        prices = data_source.get_historical_prices("SPY", 2, "day")

        # The expected df contains 2 days of data. And it is most recent from the
        # past of the requested date.
        expected_df = pd.DataFrame.from_records([
            {
                "datetime": "2024-02-05 00:00:00-05:00",
                "open": 493.65,
                "high": 494.3778,
                "low": 490.23,
                "close": 492.57,
                "volume": 74655145.0,
                "return": np.nan
            },
            {
                "datetime": "2024-02-06 00:00:00-05:00",
                "open": 492.99,
                "high": 494.3200,
                "low": 492.03,
                "close": 493.82,
                "volume": 54775803.0,
                "return": 0.0025377103761901054
            },
        ], index="datetime")
        expected_df.index = pd.to_datetime(expected_df.index).tz_convert(tzinfo)

        assert prices is not None
        assert_frame_equal(prices.df, expected_df, check_dtype=False, check_index_type=False)

    ########################################################################################
    # Below are the NEW TESTS added to verify that get_chains(), get_last_price(), and
    # get_historical_prices() data has not changed after code modifications.
    # We have NOT modified existing tests or code, only appended these tests.
    ########################################################################################

    def test_get_chains_spy_expected_data(self, monkeypatch, tmp_path):
        """
        Fast unit-level check for Polygon option chain shape + caching.

        Real Polygon chain downloads can be extremely slow for SPY (thousands of contracts),
        so this test stubs PolygonClient.list_options_contracts() and verifies:
        - The returned structure matches LumiBot's expected chain shape
        - Results are cached and reused
        """
        from lumibot.tools import polygon_helper

        monkeypatch.setattr(polygon_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))

        class _Contract:
            def __init__(self, right: str, strike: float):
                self.shares_per_contract = 100
                self.primary_exchange = "BATO"
                self.contract_type = right
                self.expiration_date = "2025-01-13"
                self.strike_price = strike

        class _Client:
            def list_options_contracts(self, **kwargs):
                # Avoid duplicates: only return contracts for non-expired queries.
                if kwargs.get("expired"):
                    return []
                return [
                    _Contract("call", 497),
                    _Contract("call", 498),
                    _Contract("put", 497),
                    _Contract("put", 498),
                ]

        current_date = datetime.date(2025, 1, 13)
        asset = Asset("SPY")
        chains = polygon_helper.get_chains_cached(
            api_key="unused",
            asset=asset,
            current_date=current_date,
            polygon_client=_Client(),
        )

        assert chains is not None
        assert chains.get("Multiplier") == 100
        assert chains.get("Exchange") == "BATO"
        assert "Chains" in chains
        assert "2025-01-13" in chains["Chains"]["CALL"]
        assert "2025-01-13" in chains["Chains"]["PUT"]

        # Verify caching: second call should reuse the parquet file and never touch the client.
        class _FailClient:
            def list_options_contracts(self, **kwargs):  # pragma: no cover
                raise AssertionError("Expected cached chain to be reused (no API calls)")

        chains_cached = polygon_helper.get_chains_cached(
            api_key="unused",
            asset=asset,
            current_date=current_date,
            polygon_client=_FailClient(),
        )
        assert chains_cached["Chains"]["CALL"]["2025-01-13"] == [497, 498]
        assert chains_cached["Chains"]["PUT"]["2025-01-13"] == [497, 498]

    @pytest.mark.skipif(not POLYGON_API_KEY or POLYGON_API_KEY == '<your key here>', reason="This test requires a Polygon.io API key")
    def test_get_last_price_unchanged(self):
        """
        Additional test to ensure get_last_price() is unaffected by code changes.
        We expect AMZN's last price (on 2024-08-02 ~10AM) to be in a certain known range
        based on historical data from Polygon.
        """
        tzinfo = pytz.timezone("America/New_York")
        start = tzinfo.localize(datetime.datetime(2024, 8, 1))
        end = tzinfo.localize(datetime.datetime(2024, 8, 4))

        data_source = PolygonDataBacktesting(start, end, api_key=POLYGON_API_KEY)
        # Pick a known date/time within our backtest window
        data_source._datetime = tzinfo.localize(datetime.datetime(2024, 8, 2, 10))

        # Trigger data fetch by calling get_historical_prices for minute bars first
        data_source.get_historical_prices("AMZN", 5, "minute")

        last_price = data_source.get_last_price(Asset("AMZN"))
        # As in the main test, we expect a price in the 160-180 range for 2024.
        assert last_price is not None, "Expected to get a price, got None"

        # AMZN price was around $161-175 on 2024-08-02
        assert 160.0 < last_price < 180.0, f"Expected AMZN price between 160 and 180 on 2024-08-02, got {last_price}"

    @pytest.mark.skipif(not POLYGON_API_KEY or POLYGON_API_KEY == '<your key here>', reason="This test requires a Polygon.io API key")
    def test_get_historical_prices_unchanged_for_amzn(self):
        """
        Additional test to ensure get_historical_prices() is unaffected by code changes.
        We'll check that we can retrieve day bars for AMZN for 2 days leading up to 2024-08-02.
        """
        tzinfo = pytz.timezone("America/New_York")
        start = datetime.datetime(2024, 8, 1).astimezone(tzinfo)
        end = datetime.datetime(2024, 8, 4).astimezone(tzinfo)

        data_source = PolygonDataBacktesting(start, end, api_key=POLYGON_API_KEY)
        # Set the 'current' backtesting datetime
        data_source._datetime = datetime.datetime(2024, 8, 2, 15).astimezone(tzinfo)

        # Trigger data fetch by calling get_historical_prices for minute bars first
        data_source.get_historical_prices("AMZN", 5, "minute")

        # Retrieve 2 day-bars for AMZN
        historical_bars = data_source.get_historical_prices("AMZN", 2, "day")
        assert historical_bars is not None, "Expected some historical bars, got None"
        df = historical_bars.df
        assert df is not None and not df.empty, "Expected non-empty DataFrame for historical AMZN day bars"
        assert len(df) == 2, f"Expected 2 day bars for AMZN, got {len(df)}"
        # Just a sanity check to make sure the close is within a plausible range (2024 AMZN prices ~160-200)
        assert df['close'].mean() < 200, "Unexpectedly high close for AMZN, data might have changed"
        assert df['close'].mean() > 150, "Unexpectedly low close for AMZN, data might have changed"
