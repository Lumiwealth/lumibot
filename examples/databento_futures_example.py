"""
DataBento Futures Trading Strategy Example

This example demonstrates how to use DataBento as a data source for futures trading
with Lumibot. It shows how to:
1. Configure DataBento as a data source
2. Create a simple futures trading strategy
3. Backtest using DataBento data

Requirements:
- DataBento API key
- databento Python package: pip install databento
"""

from datetime import datetime, timedelta
from lumibot.strategies import Strategy
from lumibot.entities import Asset
from lumibot.backtesting import DataBentoDataBacktesting


class DataBentoFuturesExample(Strategy):
    """
    Example strategy using DataBento for futures data
    
    This strategy implements a simple moving average crossover system for E-mini S&P 500 futures.
    """
    
    def initialize(self):
        """Initialize the strategy"""
        # Set the sleep time between iterations (in seconds)
        self.sleeptime = 300  # 5 minutes
        
        # Define the futures contract we want to trade
        # Using E-mini S&P 500 futures expiring in March 2025
        self.asset = Asset(
            symbol="ES",
            asset_type="future", 
            expiration=datetime(2025, 3, 21).date()  # Third Friday of March
        )
        
        # Moving average periods
        self.short_ma_period = 10
        self.long_ma_period = 30
        
        # Position sizing
        self.position_size = 1  # Number of contracts
        
        # Track last signal to avoid over-trading
        self.last_signal = None
        
        self.log_message("DataBento Futures Strategy initialized")
        self.log_message(f"Trading asset: {self.asset.symbol} expiring {self.asset.expiration}")

    def on_trading_iteration(self):
        """Main trading logic executed on each iteration"""
        
        # Get historical price data for moving averages
        bars = self.get_historical_prices(
            asset=self.asset,
            length=self.long_ma_period + 10,  # Extra buffer
            timestep="minute"
        )
        
        if bars is None or len(bars.df) < self.long_ma_period:
            self.log_message("Insufficient data for analysis")
            return
        
        # Calculate moving averages
        df = bars.df
        short_ma = df['close'].rolling(window=self.short_ma_period).mean()
        long_ma = df['close'].rolling(window=self.long_ma_period).mean()
        
        # Get current values
        current_short_ma = short_ma.iloc[-1]
        current_long_ma = long_ma.iloc[-1]
        current_price = df['close'].iloc[-1]
        
        # Get previous values for crossover detection
        prev_short_ma = short_ma.iloc[-2]
        prev_long_ma = long_ma.iloc[-2]
        
        # Determine signal
        signal = None
        
        # Bullish crossover: short MA crosses above long MA
        if prev_short_ma <= prev_long_ma and current_short_ma > current_long_ma:
            signal = "BUY"
        
        # Bearish crossover: short MA crosses below long MA
        elif prev_short_ma >= prev_long_ma and current_short_ma < current_long_ma:
            signal = "SELL"
        
        # Log current state
        self.log_message(f"Price: {current_price:.2f}, Short MA: {current_short_ma:.2f}, Long MA: {current_long_ma:.2f}")
        
        # Execute trades based on signal
        current_position = self.get_position(self.asset)
        
        if signal == "BUY" and self.last_signal != "BUY":
            if current_position:
                # Close any short position
                if current_position.quantity < 0:
                    self.sell_all(self.asset)
            
            # Open long position
            order = self.create_order(
                asset=self.asset,
                quantity=self.position_size,
                side="buy"
            )
            self.submit_order(order)
            
            self.last_signal = "BUY"
            self.log_message(f"BUY signal: Opening long position of {self.position_size} contracts")
        
        elif signal == "SELL" and self.last_signal != "SELL":
            if current_position:
                # Close any long position
                if current_position.quantity > 0:
                    self.sell_all(self.asset)
            
            # Open short position
            order = self.create_order(
                asset=self.asset,
                quantity=self.position_size,
                side="sell"
            )
            self.submit_order(order)
            
            self.last_signal = "SELL"
            self.log_message(f"SELL signal: Opening short position of {self.position_size} contracts")
        
        # Log position information
        if current_position:
            unrealized_pnl = current_position.quantity * (current_price - current_position.avg_fill_price)
            self.log_message(f"Current position: {current_position.quantity} contracts, "
                           f"Avg price: {current_position.avg_fill_price:.2f}, "
                           f"Unrealized P&L: ${unrealized_pnl:.2f}")


if __name__ == "__main__":
    """
    Example of how to backtest the strategy using DataBento data
    
    Before running this, make sure to:
    1. Install databento: pip install databento
    2. Set your DataBento API key in environment variables:
       export DATABENTO_API_KEY="your_api_key_here"
    3. Optionally set DATA_SOURCE=databento in environment variables
    """
    
    # Define backtest parameters
    backtest_start = datetime(2025, 1, 1)
    backtest_end = datetime(2025, 1, 31)
    
    # Note: You'll need a valid DataBento API key for this to work
    api_key = "your_databento_api_key_here"  # Replace with your actual API key
    
    # Create the strategy
    strategy = DataBentoFuturesExample()
    
    # Set up backtesting with DataBento data source
    strategy.backtest(
        DataBentoDataBacktesting,
        backtest_start,
        backtest_end,
        api_key=api_key,
        show_plot=True,
        show_tearsheet=True,
        save_tearsheet=True
    )
