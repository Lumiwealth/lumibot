import datetime
from lumibot.entities import Asset
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.strategies import Strategy

class PolygonBacktest(Strategy):
    parameters = {"symbol": "AMZN"}

    # Set the initial values for the strategy
    def initialize(self):
        self.sleeptime = "1D"

    # Trading Strategy: Backtest will only buy traded assets on first iteration
    def on_trading_iteration(self):
        if self.first_iteration:
            
            # Underlying Asset needs a separate test from the option test
            underlying_asset = self.parameters["symbol"]
            price = round(self.get_last_price(underlying_asset))
            qty = 10
            
            # Buy 10 shares of the underlying asset for the test
            order_underlying_asset = self.create_order(underlying_asset, quantity=qty, side="buy")
            self.submit_order(order_underlying_asset)
            
            # Create simple option chain | Plugging Amazon "AMZN"; always checking Friday (08/04/23) ensuring Traded_asset exists
            option_asset = Asset(
                symbol=underlying_asset,
                asset_type="option",
                expiration="08-04-2023",  # Polygon and Lumibot (pandas) both use "YYYY-MM-DD" format
                right="CALL",
                strike=price,
                multiplier=100,
                currency="USD"
            )
            
            # Buy 1 option contract for the test
            order_option_asset = self.create_order(option_asset, quantity=1, side="buy")
            self.submit_order(order_option_asset)           
    
    # The main function where the execution of the strategy is held
    def main_function():
        """main: this function is the main entry point and execution for Live or Backtesting of the strategy_class"""
        
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        symbol = "AMZN"
        strategy_class = PolygonBacktest
        underlying_asset = Asset(symbol=symbol, asset_type="stock")
        backtesting_start = datetime.datetime(2023, 8, 1)
        backtesting_end = datetime.datetime(2023, 8, 4)

        # Execute Backtest | Polygon.io API Connection
        strategy_class.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            quote_asset=underlying_asset,
            benchmark_asset = "SPY",
            polygon_api_key="ouMFLGcPY21oVnLTGh6w4QorURSTNOE5",  # TODO Replace with Lumibot owned API Key
            polygon_has_paid_subscription=False,
    )

    # Primary Function: Execute Main Function
    if __name__ == "__main__":
        "Runs the main function"
        main_function()