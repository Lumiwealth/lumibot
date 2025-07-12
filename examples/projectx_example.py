"""
ProjectX Example Strategy for Lumibot

This example demonstrates how to use the ProjectX broker integration
for futures trading with Lumibot. ProjectX supports multiple underlying
futures brokers (TSX, TOPONE, etc.) through a unified API.

Environment Variables Required:
- PROJECTX_FIRM: Broker name (e.g., "TSX", "TOPONE")
- PROJECTX_API_KEY: Your API key for the broker
- PROJECTX_USERNAME: Your username for the broker  
- PROJECTX_BASE_URL: Base URL for the broker API
- PROJECTX_PREFERRED_ACCOUNT_NAME: (Optional) Preferred account name

Example .env file:
PROJECTX_FIRM=TSX
PROJECTX_API_KEY=your_api_key_here
PROJECTX_USERNAME=your_username_here
PROJECTX_BASE_URL=https://api.yourbroker.com
PROJECTX_PREFERRED_ACCOUNT_NAME=Practice-Account-1
"""

import logging

from lumibot.brokers import ProjectX
from lumibot.data_sources import ProjectXData
from lumibot.entities import Asset
from lumibot.strategies import Strategy


class ProjectXFuturesStrategy(Strategy):
    """
    Example strategy using ProjectX broker for futures trading.
    
    This strategy demonstrates:
    - Connecting to ProjectX broker
    - Creating futures assets
    - Placing market and limit orders
    - Managing positions
    - Getting market data
    """
    
    # Strategy parameters
    parameters = {
        "symbol": "ES",  # E-mini S&P 500 futures
        "quantity": 1,   # Number of contracts to trade
        "take_profit_percent": 0.02,  # 2% take profit
        "stop_loss_percent": 0.01,    # 1% stop loss
        "lookback_period": 20,        # Lookback period for moving average
    }
    
    def initialize(self):
        """Initialize the strategy."""
        # Set trading frequency
        self.sleeptime = "1M"  # Check every minute
        
        # Create the futures asset
        self.asset = Asset(self.parameters["symbol"], asset_type="future")
        
        # Track our position
        self.position_size = 0
        self.entry_price = None
        
        # Track moving average for trend
        self.price_history = []
        
        logging.info(f"Initialized ProjectX strategy for {self.parameters['symbol']}")
    
    def on_trading_iteration(self):
        """Main trading logic executed each iteration."""
        try:
            # Get current price
            current_price = self.get_last_price(self.asset)
            if current_price is None:
                self.log_message("Could not get current price, skipping iteration")
                return
            
            # Update price history for moving average calculation
            self.price_history.append(current_price)
            if len(self.price_history) > self.parameters["lookback_period"]:
                self.price_history.pop(0)
            
            # Calculate moving average if we have enough data
            if len(self.price_history) >= self.parameters["lookback_period"]:
                moving_average = sum(self.price_history) / len(self.price_history)
                
                # Get current position
                position = self.get_position(self.asset)
                current_quantity = int(position.quantity) if position else 0
                
                self.log_message(f"Current price: {current_price:.2f}, MA: {moving_average:.2f}, Position: {current_quantity}")
                
                # Trading logic
                if current_quantity == 0:
                    # No position - look for entry signals
                    if current_price > moving_average:
                        # Price above MA - go long
                        self.log_message(f"Price above MA, going long {self.parameters['quantity']} contracts")
                        self._enter_long_position(current_price)
                    elif current_price < moving_average:
                        # Price below MA - go short
                        self.log_message(f"Price below MA, going short {self.parameters['quantity']} contracts")
                        self._enter_short_position(current_price)
                
                elif current_quantity > 0:
                    # Long position - check exit conditions
                    if self.entry_price:
                        profit_pct = (current_price - self.entry_price) / self.entry_price
                        
                        if profit_pct >= self.parameters["take_profit_percent"]:
                            self.log_message(f"Take profit triggered: {profit_pct:.2%}")
                            self._close_position()
                        elif profit_pct <= -self.parameters["stop_loss_percent"]:
                            self.log_message(f"Stop loss triggered: {profit_pct:.2%}")
                            self._close_position()
                        elif current_price < moving_average:
                            self.log_message("Price below MA, closing long position")
                            self._close_position()
                
                elif current_quantity < 0:
                    # Short position - check exit conditions
                    if self.entry_price:
                        profit_pct = (self.entry_price - current_price) / self.entry_price
                        
                        if profit_pct >= self.parameters["take_profit_percent"]:
                            self.log_message(f"Take profit triggered: {profit_pct:.2%}")
                            self._close_position()
                        elif profit_pct <= -self.parameters["stop_loss_percent"]:
                            self.log_message(f"Stop loss triggered: {profit_pct:.2%}")
                            self._close_position()
                        elif current_price > moving_average:
                            self.log_message("Price above MA, closing short position")
                            self._close_position()
            
            else:
                self.log_message(f"Building price history: {len(self.price_history)}/{self.parameters['lookback_period']}")
        
        except Exception as e:
            self.log_message(f"Error in trading iteration: {e}")
    
    def _enter_long_position(self, current_price):
        """Enter a long position."""
        order = self.create_order(
            asset=self.asset,
            quantity=self.parameters["quantity"],
            side="buy",
            type="market"
        )
        self.submit_order(order)
        self.entry_price = current_price
    
    def _enter_short_position(self, current_price):
        """Enter a short position."""
        order = self.create_order(
            asset=self.asset,
            quantity=self.parameters["quantity"],
            side="sell",
            type="market"
        )
        self.submit_order(order)
        self.entry_price = current_price
    
    def _close_position(self):
        """Close the current position."""
        position = self.get_position(self.asset)
        if position and position.quantity != 0:
            # Determine the side to close the position
            side = "sell" if position.quantity > 0 else "buy"
            quantity = abs(int(position.quantity))
            
            order = self.create_order(
                asset=self.asset,
                quantity=quantity,
                side=side,
                type="market"
            )
            self.submit_order(order)
            self.entry_price = None
    
    def on_abrupt_closing(self):
        """Handle strategy shutdown."""
        self.log_message("Strategy shutting down, closing any open positions")
        self._close_position()


def main():
    """
    Run the ProjectX futures trading strategy.
    
    Make sure you have set up your environment variables:
    - PROJECTX_FIRM
    - PROJECTX_API_KEY  
    - PROJECTX_USERNAME
    - PROJECTX_BASE_URL
    - PROJECTX_PREFERRED_ACCOUNT_NAME (optional)
    """
    # Create ProjectX data source first
    data_source = ProjectXData()
    
    # Create ProjectX broker with data source
    broker = ProjectX(data_source=data_source)
    
    # Create and run the strategy
    strategy = ProjectXFuturesStrategy(
        broker=broker,
        data_source=data_source
    )
    
    # Run the strategy
    strategy.run_backtest(
        show_plot=True,
        show_tearsheet=True,
        show_indicators=True,
    )


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    main() 