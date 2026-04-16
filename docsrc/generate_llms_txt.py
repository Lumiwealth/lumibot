#!/usr/bin/env python3
"""Generate llms.txt index file for AI discoverability."""

import datetime
import sys
from pathlib import Path

LLMS_TXT_CONTENT = '''# Lumibot

> Python trading and backtesting framework for stocks, options, crypto, and futures.
> Supports Alpaca, Interactive Brokers, Tradier, Schwab, ThetaData, Yahoo Finance, and Polygon.
> Includes built-in AI trading agents, agentic backtesting, DuckDB query tools, replay caching, and external MCP tool support.

## Critical Rules for Code Generation

- **NEVER** use `datetime.now()` or `datetime.today()` - always use `self.get_datetime()` for backtesting compatibility
- **NEVER** use `from __future__ import annotations` - it breaks Lumibot's type checking
- Use `self.vars` for persistent variables across lifecycle methods (e.g., `self.vars.my_variable = value`)
- Use `self.log_message()` instead of `print()` for proper logging
- Get current prices with `self.get_last_price(asset)` - returns None if unavailable
- Submit orders with `self.submit_order(order)` where `order = self.create_order(asset, quantity, side)`
- Access portfolio with `self.portfolio_value`, `self.cash`, `self.positions`
- Implement `on_trading_iteration()` for main strategy logic - runs once per bar/iteration
- For options: Use `self.create_asset(symbol, asset_type=Asset.AssetType.OPTION, expiration=date, strike=price, right='call'|'put')`
- AI agents live on `self.agents` and should be created in `initialize()`
- Use DuckDB for agent time-series analysis instead of pasting large historical bar payloads into prompts

## Full Documentation

For complete API documentation with all method signatures, parameters, return types, and examples, see **llms-full.txt** in this repository.

## AI Trading Agents

- LumiBot supports **AI trading agents** directly inside the `Strategy` lifecycle with `self.agents.create(...)`
- Agents can run from `initialize()`, `on_trading_iteration()`, `on_filled_order()`, and other lifecycle methods
- Agentic backtests can replay identical runs from cache without another model call
- DuckDB is the built-in SQL query surface for time-series analysis
- External MCP servers can be mounted with explicit tool allowlists
- Main docs page: `https://lumibot.lumiwealth.com/agents.html`
- Backtesting docs: `https://lumibot.lumiwealth.com/backtesting.html`
- GitHub guide: `https://github.com/Lumiwealth/lumibot/blob/dev/docs/AI_TRADING_AGENTS.md`

## Quick Reference

### Lifecycle Methods
- `initialize()` - Called once at strategy start, set up variables here
- `on_trading_iteration()` - Main strategy logic, runs every bar/iteration
- `before_market_opens()` - Called before market opens each day
- `before_market_closes()` - Called before market closes each day
- `after_market_closes()` - Called after market closes each day
- `on_filled_order(position, order, price, quantity)` - Called when order fills
- `trace_stats(context, snapshot_before)` - Log custom stats each iteration

### Order Methods
- `self.create_order(asset, quantity, side, **kwargs)` - Create an order object
- `self.submit_order(order)` - Submit order for execution
- `self.cancel_order(order)` - Cancel a pending order
- `self.sell_all()` - Liquidate all positions
- `self.get_orders()` - Get all orders
- `self.get_order(identifier)` - Get specific order by ID

### Data Methods
- `self.get_last_price(asset)` - Get current/last price
- `self.get_historical_prices(asset, length, timestep)` - Get OHLCV bars
- `self.get_historical_prices_for_assets(assets, length, timestep)` - Get bars for multiple assets
- `self.get_quote(asset)` - Get current quote (bid/ask)

### Account Methods
- `self.get_cash()` - Get available cash
- `self.get_portfolio_value()` - Get total portfolio value
- `self.get_position(asset)` - Get position for asset
- `self.get_positions()` - Get all positions

### DateTime Methods (USE THESE, not datetime.now())
- `self.get_datetime()` - Get current datetime (backtesting-safe)
- `self.get_timestamp()` - Get current timestamp
- `self.get_round_minute(timeshift)` - Get rounded minute
- `self.get_round_day(timeshift)` - Get rounded day

### Options Methods
- `self.get_chains(asset)` - Get option chains
- `self.get_chain(chains, exchange)` - Get chain for specific exchange
- `self.get_strikes(chain)` - Get available strikes
- `self.get_expiration(chain, expiration_date)` - Get specific expiration
- `self.get_greeks(asset)` - Get option greeks

### Key Properties
- `self.cash` - Current cash balance
- `self.portfolio_value` - Total portfolio value
- `self.positions` - Dict of current positions
- `self.first_iteration` - True on first iteration
- `self.is_backtesting` - True if backtesting
- `self.minutes_before_closing` - Minutes before market close
- `self.sleeptime` - Seconds between iterations

### Asset Creation
```python
# Stock
asset = Asset(symbol="AAPL", asset_type=Asset.AssetType.STOCK)

# Option
asset = Asset(
    symbol="AAPL",
    asset_type=Asset.AssetType.OPTION,
    expiration=datetime.date(2024, 1, 19),
    strike=150,
    right="call"  # or "put"
)

# Crypto
asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)

# Future
asset = Asset(symbol="ES", asset_type=Asset.AssetType.FUTURE, expiration=datetime.date(2024, 3, 15))
```

### Basic Strategy Template
```python
from lumibot.strategies import Strategy
from lumibot.entities import Asset

class MyStrategy(Strategy):
    parameters = {"symbol": "AAPL", "quantity": 10}

    def initialize(self):
        self.sleeptime = "1D"  # Run once per day

    def on_trading_iteration(self):
        symbol = self.parameters["symbol"]
        qty = self.parameters["quantity"]

        # Get current price (use self.get_datetime(), NOT datetime.now())
        price = self.get_last_price(symbol)
        if price is None:
            self.log_message(f"No price for {symbol}")
            return

        # Check position
        position = self.get_position(symbol)
        if position is None:
            # Buy if no position
            order = self.create_order(symbol, qty, "buy")
            self.submit_order(order)
```

## Backtesting Data Sources
- `YahooDataBacktesting` - Free, good for stocks
- `PolygonDataBacktesting` - Crypto and stocks (requires API key)
- `ThetaDataBacktesting` - Options and stocks (requires subscription)

Set via environment variable: `BACKTESTING_DATA_SOURCE=yahoo|polygon|thetadata`
'''


def main():
    output_path = Path(__file__).parent.parent / "llms.txt"

    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    content = LLMS_TXT_CONTENT.strip() + f"\n\n# Generated: {timestamp}\n"

    output_path.write_text(content)
    print(f"llms.txt generated at {output_path} ({len(content)} bytes)")


if __name__ == "__main__":
    main()
