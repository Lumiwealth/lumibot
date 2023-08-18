import datetime
import os

import pytest
import pytz
from lumibot.entities import Asset
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.strategies import Strategy


# Lumibot doesn't allow any other non-global hooks for storing data during backtesting
ORDERS = []
PRICES = {}

# API Key for Polygon.io
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")


class PolygonBacktestStrat(Strategy):
    parameters = {"symbol": "AMZN"}

    # Set the initial values for the strategy
    def initialize(self):
        self.sleeptime = "1D"
        self.first_price = None
        self.first_option_price = None
        self.orders = []

    # Trading Strategy: Backtest will only buy traded assets on first iteration
    def on_trading_iteration(self):
        if self.first_iteration:
            now = self.get_datetime()

            # Create simple option chain | Plugging Amazon "AMZN"; always checking Friday (08/04/23) ensuring
            # Traded_asset exists
            underlying_asset = self.parameters["symbol"]
            current_asset_price = self.get_last_price(underlying_asset)
            strike_price = round(current_asset_price)
            option_asset = Asset(
                symbol=underlying_asset,
                asset_type="option",
                expiration=datetime.date(2023, 8, 4),
                right="CALL",
                strike=strike_price,
                multiplier=100,
                currency="USD",
            )
            current_option_price = self.get_last_price(option_asset)

            # Buy 10 shares of the underlying asset for the test
            qty = 10
            self.log_message(f"Buying {qty} shares of {underlying_asset} at {current_asset_price} @ {now}")
            order_underlying_asset = self.create_order(underlying_asset, quantity=qty, side="buy")
            submitted_order = self.submit_order(order_underlying_asset)
            ORDERS.append(submitted_order)
            PRICES[submitted_order.identifier] = current_asset_price

            # Buy 1 option contract for the test
            order_option_asset = self.create_order(option_asset, quantity=1, side="buy")
            submitted_order = self.submit_order(order_option_asset)
            ORDERS.append(submitted_order)
            PRICES[submitted_order.identifier] = current_option_price


class TestPolygonBacktestFull:
    def test_polygon_restclient(self):
        """
        Test Polygon REST Client with Lumibot Backtesting and real API calls to Polygon. Using the Amazon stock
        which only has options expiring on Fridays. This test will buy 10 shares of Amazon and 1 option contract
        in the historical 2023-08-04 period (in the past!).
        """
        
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        symbol = "AMZN"
        underlying_asset = Asset(symbol=symbol, asset_type="stock")
        backtesting_start = datetime.datetime(2023, 8, 1)
        backtesting_end = datetime.datetime(2023, 8, 4)

        # Execute Backtest | Polygon.io API Connection
        results = PolygonBacktestStrat.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            polygon_api_key=POLYGON_API_KEY,  # TODO Replace with Lumibot owned API Key
            # Painfully slow with free subscription setting b/c lumibot is over querying and imposing a very
            # strict rate limit
            polygon_has_paid_subscription=True,
        )

        assert results
        assert len(ORDERS) == 2
        asset_order_id = ORDERS[0].identifier
        option_order_id = ORDERS[1].identifier
        assert asset_order_id in PRICES
        assert option_order_id in PRICES
        assert 130.0 < PRICES[asset_order_id] < 140.0, "Valid asset price should be between 130 and 140 for time period"
        assert 3.5 < PRICES[option_order_id] < 4.5, "Valid option price should be between 3.5 and 4.5 for time period"


@pytest.mark.skip("DataSource is not working well outside of a full backtest")
class TestPolygonBacktestBasics:
    def test_polygon_basics(self):
        asset = Asset("SPY")
        now = datetime.datetime.now(pytz.utc)
        start = now - datetime.timedelta(days=1)
        end = now
        polygon_backtest = PolygonDataBacktesting(
            start,
            end,
            polygon_api_key=POLYGON_API_KEY,
            has_paid_subscription=True,
        )
        assert polygon_backtest.get_last_price(asset)
