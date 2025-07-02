# ProjectX Integration Guide for Lumibot

## Overview

ProjectX integration provides Lumibot with access to multiple futures brokers through a unified API gateway. This integration supports popular futures brokers like TSX, TOPONE, and others, allowing you to trade futures contracts seamlessly within the Lumibot framework.

## Features

### Broker Features
- **Multi-broker support**: Connect to TSX, TOPONE, and other supported futures brokers
- **Order management**: Market, limit, stop, and other order types
- **Position tracking**: Real-time position monitoring
- **Account management**: Multiple account support with automatic selection
- **Real-time streaming**: Live order, position, and trade updates via SignalR

### Data Source Features
- **Historical data**: OHLCV bars with multiple timeframes
- **Real-time pricing**: Current market prices for futures contracts
- **Contract search**: Find and validate futures contracts
- **Multiple timeframes**: Minute, hour, daily, weekly, and monthly data

## Installation

### Dependencies
ProjectX integration requires the `signalrcore` package for streaming functionality:

```bash
pip install signalrcore>=0.9.2
```

This dependency is automatically included in Lumibot's requirements.txt.

## Configuration

### Environment Variables
Set up your ProjectX credentials using environment variables:

```bash
# Required
PROJECTX_FIRM=TSX                                    # Your broker name
PROJECTX_API_KEY=your_api_key_here                  # Your API key
PROJECTX_USERNAME=your_username_here                # Your username
PROJECTX_BASE_URL=https://api.yourbroker.com        # Broker API URL

# Optional
PROJECTX_PREFERRED_ACCOUNT_NAME=Practice-Account-1  # Preferred account name
```

### .env File Example
Create a `.env` file in your project root:

```env
# ProjectX Configuration
PROJECTX_FIRM=TSX
PROJECTX_API_KEY=your_api_key_here
PROJECTX_USERNAME=your_username_here
PROJECTX_BASE_URL=https://api.yourbroker.com
PROJECTX_PREFERRED_ACCOUNT_NAME=Practice-Account-1

# Lumibot Configuration
IS_BACKTESTING=false
TRADING_BROKER=projectx
DATA_SOURCE=projectx
```

### Broker-Specific Configuration Examples

#### TSX
```env
PROJECTX_FIRM=TSX
PROJECTX_BASE_URL=https://api.tsx.com
```

#### TOPONE
```env
PROJECTX_FIRM=TOPONE
PROJECTX_BASE_URL=https://api.topone.com
```

## Usage

### Basic Strategy Setup

```python
from lumibot.brokers import ProjectX
from lumibot.data_sources import ProjectXData
from lumibot.entities import Asset
from lumibot.strategies import Strategy

class MyFuturesStrategy(Strategy):
    def initialize(self):
        # Create futures asset
        self.asset = Asset("ES", asset_type="future")  # E-mini S&P 500
        
    def on_trading_iteration(self):
        # Get current price
        current_price = self.get_last_price(self.asset)
        
        # Trading logic here
        if self.should_buy():
            order = self.create_order(
                asset=self.asset,
                quantity=1,
                side="buy",
                type="market"
            )
            self.submit_order(order)

# Create broker and data source
broker = ProjectX()
data_source = ProjectXData()

# Run strategy
strategy = MyFuturesStrategy(broker=broker, data_source=data_source)
strategy.run()
```

### Using Environment Variables (Recommended)

```python
# With environment variables set, you can use:
from lumibot.strategies import Strategy

class MyStrategy(Strategy):
    # Your strategy code here
    pass

# Lumibot will automatically use ProjectX if configured
strategy = MyStrategy()
strategy.run()
```

### Advanced Configuration

```python
# Custom configuration (overrides environment variables)
config = {
    "firm": "TSX",
    "api_key": "your_api_key",
    "username": "your_username",
    "base_url": "https://api.tsx.com",
    "preferred_account_name": "Practice-Account-1"
}

broker = ProjectX(config=config)
data_source = ProjectXData(config=config)
```

## Supported Order Types

| Order Type    | ProjectX Value | Description           |
|---------------|----------------|-----------------------|
| `market`      | 1              | Market order          |
| `limit`       | 2              | Limit order           |
| `stop`        | 3              | Stop order            |
| `stop_limit`  | 4              | Stop limit order      |
| `trail`       | 5              | Trailing stop order   |
| `trail_limit` | 6              | Trailing stop limit   |

## Asset Types

ProjectX primarily supports futures contracts:

```python
# Create futures assets
es_future = Asset("ES", asset_type="future")      # E-mini S&P 500
nq_future = Asset("NQ", asset_type="future")      # E-mini NASDAQ
ym_future = Asset("YM", asset_type="future")      # E-mini Dow Jones
```

## Data Source Usage

### Getting Historical Data

```python
from lumibot.data_sources import ProjectXData
from lumibot.entities import Asset

data_source = ProjectXData()
asset = Asset("ES", asset_type="future")

# Get last 100 daily bars
bars = data_source.get_bars(
    asset=asset,
    length=100,
    timespan="day"
)

# Get hourly bars
bars = data_source.get_bars(
    asset=asset,
    length=50,
    timespan="hour"
)

# Get minute bars
bars = data_source.get_bars(
    asset=asset,
    length=1000,
    timespan="minute"
)
```

### Supported Timeframes

- `minute` or `1minute`, `5minute`, etc.
- `hour` or `1hour`, `4hour`, etc.
- `day` or `1day`
- `week` or `1week`
- `month` or `1month`

### Contract Search

```python
# Search for contracts
contracts = data_source.search_contracts("ES")
print(f"Found {len(contracts)} ES contracts")

# Get contract details
details = data_source.get_contract_details(asset)
print(f"Tick size: {details.get('tickSize')}")
```

## Error Handling

### Common Issues and Solutions

#### Authentication Errors
```python
# Check your credentials
config = {
    "firm": "TSX",
    "api_key": "correct_api_key",
    "username": "correct_username",
    "base_url": "https://correct-api-url.com"
}
```

#### Contract Not Found
```python
# Search for available contracts first
data_source = ProjectXData()
contracts = data_source.search_contracts("ES")
if contracts:
    contract_id = contracts[0]["id"]
    symbol = contracts[0]["symbol"]
    asset = Asset(symbol, asset_type="future")
```

#### Connection Issues
```python
# Check broker connection
broker = ProjectX()
if broker.connect():
    print("Connected successfully")
else:
    print("Connection failed - check configuration")
```

## Streaming Data

ProjectX supports real-time streaming of:
- Order updates
- Position changes
- Trade executions
- Account information

Streaming is automatically enabled by default and handled internally by the broker.

## Best Practices

### 1. Account Selection
- Use `PROJECTX_PREFERRED_ACCOUNT_NAME` to specify which account to use
- The broker will automatically select practice accounts when available

### 2. Price Precision
- ProjectX automatically handles tick size rounding for orders
- Contract tick sizes are retrieved automatically

### 3. Error Handling
```python
def on_trading_iteration(self):
    try:
        current_price = self.get_last_price(self.asset)
        if current_price is None:
            self.log_message("Could not get price, skipping iteration")
            return
        
        # Your trading logic here
        
    except Exception as e:
        self.log_message(f"Error in trading iteration: {e}")
```

### 4. Position Management
```python
def close_all_positions(self):
    """Close all open positions."""
    positions = self.get_positions()
    for position in positions:
        if position.quantity != 0:
            side = "sell" if position.quantity > 0 else "buy"
            quantity = abs(int(position.quantity))
            
            order = self.create_order(
                asset=position.asset,
                quantity=quantity,
                side=side,
                type="market"
            )
            self.submit_order(order)
```

## Example Strategies

See the `examples/projectx_example.py` file for a complete futures trading strategy that demonstrates:
- Moving average crossover strategy
- Risk management with stop losses and take profits
- Position sizing and management
- Error handling

## Troubleshooting

### Check Configuration
```python
from lumibot.credentials import PROJECTX_CONFIG
print("ProjectX Config:", PROJECTX_CONFIG)
```

### Test Connection
```python
from lumibot.brokers import ProjectX

broker = ProjectX()
if broker.connect():
    print("Connection successful!")
    account_id = broker.account_id
    print(f"Using account: {account_id}")
else:
    print("Connection failed")
```

### Enable Debug Logging
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Limitations

1. **Options Trading**: ProjectX is a futures broker and does not support options trading
2. **Dividend Data**: Futures contracts do not have dividends
3. **Options Chains**: Not applicable for futures contracts
4. **Real-time Market Data**: Basic implementation using last price; full market data requires streaming

## Support

For ProjectX-specific issues:
1. Check your broker's API documentation
2. Verify your credentials and API access
3. Ensure your account has trading permissions
4. Contact your broker's technical support for API issues

For Lumibot integration issues:
1. Check the logs for detailed error messages
2. Verify all required environment variables are set
3. Test with the provided example strategy
4. Check the Lumibot documentation for general usage questions 