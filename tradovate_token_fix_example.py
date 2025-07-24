#!/usr/bin/env python3
"""
Example of how to use the updated Tradovate broker with automatic token renewal.

This demonstrates two approaches:
1. Automatic renewal on 401 errors (built into the broker)
2. Proactive token renewal before expiry
"""

from datetime import datetime
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset
from lumibot.credentials import IS_BACKTESTING
import time

class TokenRenewalExample(Strategy):
    """
    Example strategy that demonstrates token renewal functionality.
    """
    parameters = {
        "token_check_interval": 3600,  # Check token every 60 minutes (tokens expire at 80 mins)
    }
    
    def initialize(self):
        self.asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        self.sleeptime = "1M"
        self.set_market("us_futures")
        self.last_token_check = time.time()
        
    def on_trading_iteration(self):
        # Proactive token renewal check
        current_time = time.time()
        if current_time - self.last_token_check > self.parameters["token_check_interval"]:
            self.log_message("Performing proactive token renewal check...")
            if hasattr(self.broker, 'check_token_expiry'):
                self.broker.check_token_expiry()
            self.last_token_check = current_time
        
        # Your normal trading logic here
        self.log_message(f"Trading iteration at {self.get_datetime()}")
        
        # This will automatically handle token renewal on 401 errors
        try:
            cash, positions_value, portfolio_value = self.get_cash()
            self.log_message(f"Portfolio value: ${portfolio_value:.2f}")
        except Exception as e:
            self.log_message(f"Error getting balances: {e}", color="red")

if __name__ == "__main__":
    if not IS_BACKTESTING:
        trader = Trader()
        strategy = TokenRenewalExample(
            quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX)
        )
        trader.add_strategy(strategy)
        trader.run_all()
    else:
        print("This example is for live trading only.")