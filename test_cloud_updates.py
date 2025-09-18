#!/usr/bin/env python3
"""
Test strategy to verify cloud updates work during market closed periods.
This strategy doesn't trade - it just holds positions and sends cloud updates.
"""

import os
from datetime import datetime, timedelta
from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset


class CloudUpdateTestStrategy(Strategy):
    def initialize(self):
        # Very short sleep time to test more frequently
        self.sleeptime = 1  # Check every 1 minute

        # Set API key for cloud updates
        self.lumiwealth_api_key = "5fafe0014a854404a83f7775fe9ea6ae86895571b8cfb3c5"

        self.log_message("🚀 CloudUpdateTestStrategy initialized with API key", color="green")
        self.log_message(f"🔑 Using API key: {self.lumiwealth_api_key[:20]}...", color="blue")

    def on_trading_iteration(self):
        # Don't actually trade - just log that we're running
        self.log_message("🔄 Trading iteration running (but not trading)", color="cyan")

        # Get current portfolio value
        portfolio_value = self.get_portfolio_value()
        positions = self.get_positions()

        self.log_message(f"📊 Portfolio value: ${portfolio_value:.2f}, Positions: {len(positions)}", color="blue")


if __name__ == "__main__":
    print("🧪 Starting Cloud Update Test Strategy...")
    print("📝 This will test if cloud updates work during market closed periods")
    print("⏰ Watch for '☁️ Sending cloud update' messages every minute")
    print("🛑 Press Ctrl+C to stop")
    print()

    # Use BacktestingBroker to avoid needing real broker credentials
    # But configure it for live-like behavior
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
    data_source = YahooDataBacktesting(start_date, end_date)
    broker = BacktestingBroker(data_source)

    # Create and run the strategy
    strategy = CloudUpdateTestStrategy(broker)

    try:
        strategy.run_live()
    except KeyboardInterrupt:
        print("\n🛑 Test stopped by user")