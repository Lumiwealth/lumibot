# DataBento Integration with Lumibot

This document describes how to use DataBento as a data source in Lumibot for trading strategies, particularly for futures data.

## Overview

DataBento provides institutional-grade market data through their API. This integration allows Lumibot strategies to access DataBento's high-quality historical and real-time data for backtesting and live trading.

## Features

- **Futures Data Support**: Primary focus on futures contracts from major exchanges
- **Multiple Asset Types**: Extensible to stocks, options, and other instruments
- **Caching**: Intelligent local caching to minimize API calls and costs
- **Error Handling**: Robust retry logic and error management
- **Backtesting**: Full integration with Lumibot's backtesting framework

## Installation

1. Install the DataBento Python package:
   ```bash
   pip install databento
   ```

2. Obtain a DataBento API key from [DataBento](https://databento.com/)

## Configuration

### Environment Variables

Set the following environment variables:

```bash
# Required
export DATABENTO_API_KEY="your_api_key_here"

# Optional
export DATABENTO_TIMEOUT="30"
export DATABENTO_MAX_RETRIES="3"
export DATA_SOURCE="databento"  # To use DataBento as the default data source
```

### Configuration in Strategy

You can also configure DataBento directly in your strategy code:

```python
from lumibot.data_sources import DataBentoData

# Create DataBento data source
data_source = DataBentoData(
    api_key="your_api_key_here",
    timeout=30,
    max_retries=3
)
```

## Usage

### Basic Strategy Example

```python
from datetime import datetime
from lumibot.strategies import Strategy
from lumibot.entities import Asset
from lumibot.backtesting import DataBentoDataBacktesting

class MyFuturesStrategy(Strategy):
    def initialize(self):
        # Define a futures asset
        self.asset = Asset(
            symbol="ES",  # E-mini S&P 500
            asset_type="future",
            expiration=datetime(2025, 3, 21).date()
        )
    
    def on_trading_iteration(self):
        # Get historical data
        bars = self.get_historical_prices(
            asset=self.asset,
            length=20,
            timestep="minute"
        )
        
        # Your trading logic here
        current_price = bars.df['close'].iloc[-1]
        # ...

# Backtesting
strategy = MyFuturesStrategy()
strategy.backtest(
    DataBentoDataBacktesting,
    datetime(2025, 1, 1),
    datetime(2025, 1, 31),
    api_key="your_api_key_here"
)
```

### Supported Asset Types

#### Futures Contracts

```python
# Specific contract with expiration
es_contract = Asset(
    symbol="ES",
    asset_type="future",
    expiration=datetime(2025, 3, 21).date()
)

# Continuous contract (no expiration)
es_continuous = Asset(
    symbol="ES",
    asset_type="future"
)
```

#### Equity Data

```python
# Stock symbol
apple_stock = Asset(
    symbol="AAPL",
    asset_type="stock"
)
```

### Timesteps

DataBento integration supports multiple timesteps:

- `"minute"` or `"1m"` - 1-minute bars
- `"hour"` or `"1h"` - 1-hour bars  
- `"day"` or `"1d"` - Daily bars

### Venues/Exchanges

You can specify the exchange/venue for more precise data:

```python
bars = self.get_historical_prices(
    asset=asset,
    length=100,
    timestep="minute",
    exchange="CME"  # Chicago Mercantile Exchange
)
```

Supported venues for futures:
- `"CME"` - Chicago Mercantile Exchange
- `"CBOT"` - Chicago Board of Trade
- `"NYMEX"` - New York Mercantile Exchange
- `"COMEX"` - Commodity Exchange
- `"ICE"` - Intercontinental Exchange

## Data Schemas

DataBento uses different schemas for different data types:

- `ohlcv-1m` - 1-minute OHLCV data
- `ohlcv-1h` - 1-hour OHLCV data
- `ohlcv-1d` - Daily OHLCV data

The integration automatically maps Lumibot timesteps to appropriate DataBento schemas.

## Caching

DataBento data is cached locally to minimize API calls and reduce costs:

- Cache location: `~/.lumibot/cache/databento/`
- Cache format: Feather files for fast I/O
- Cache invalidation: Based on data freshness

To force cache refresh:

```python
bars = data_source.get_historical_prices(
    asset=asset,
    length=100,
    force_cache_update=True
)
```

## Error Handling

The integration includes comprehensive error handling:

- **Retry Logic**: Automatic retries with exponential backoff
- **Rate Limiting**: Respects DataBento API limits
- **Error Logging**: Detailed logging for debugging
- **Graceful Degradation**: Handles missing data gracefully

## Cost Management

To minimize DataBento API costs:

1. **Use Caching**: Data is cached automatically
2. **Batch Requests**: Request larger date ranges when possible
3. **Monitor Usage**: Track API calls in logs
4. **Test with Small Datasets**: Use shorter backtests during development

## Limitations

1. **Options Chains**: DataBento doesn't provide options chain data
2. **Real-time Data**: Current implementation focuses on historical data
3. **Symbol Mapping**: May require custom symbol formatting for some instruments

## Troubleshooting

### Common Issues

1. **Import Error**: "DataBento package not available"
   - Solution: Install databento package with `pip install databento`

2. **Authentication Error**: "Missing DataBento credentials"
   - Solution: Set `DATABENTO_API_KEY` environment variable

3. **No Data Returned**: Empty DataFrames
   - Check symbol format and date ranges
   - Verify DataBento has data for the requested instrument/period
   - Check API key permissions

4. **Rate Limiting**: Too many API requests
   - Increase retry delays
   - Use larger batch sizes
   - Enable caching

### Debug Logging

Enable debug logging to troubleshoot issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Support

For DataBento-specific issues:
- [DataBento Documentation](https://docs.databento.com/)
- [DataBento Support](mailto:support@databento.com)

For Lumibot integration issues:
- Check the logs for detailed error messages
- Verify environment variables are set correctly
- Test with the provided example strategies

## Example Files

- `examples/databento_futures_example.py` - Basic futures trading strategy
- `tests/test_databento_*.py` - Unit tests for DataBento integration
