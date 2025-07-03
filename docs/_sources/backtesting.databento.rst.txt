DataBento Backtesting
*******************

DataBento is a premium financial data provider that offers high-quality, clean market data for backtesting. Lumibot integrates with DataBento to provide reliable historical data for stocks, futures, options, and other instruments.

Overview
========

DataBento provides:

- **High-quality historical data** with minimal gaps or errors
- **Multiple timeframes** from tick-level to daily data
- **Extensive instrument coverage** including stocks, futures, and options
- **Clean data processing** with corporate action adjustments
- **API-based access** for automated data retrieval

Setting Up DataBento
====================

1. **Get DataBento API Key**
   
   Visit `DataBento <https://databento.com>`_ to sign up and get your API key.

2. **Install Dependencies**
   
   DataBento support is included with Lumibot, but you may need to install additional dependencies:

   .. code-block:: bash

       pip install databento

3. **Configure API Key**
   
   Set your DataBento API key in your environment or strategy:

   .. code-block:: python

       import os
       os.environ['DATABENTO_API_KEY'] = 'your_api_key_here'

   Or create a ``.env`` file:

   .. code-block:: bash

       DATABENTO_API_KEY=your_api_key_here

Basic Usage
===========

Here's how to use DataBento for backtesting:

.. code-block:: python

    from lumibot.strategies import Strategy
    from lumibot.entities import Asset
    from lumibot.backtesting import DataBentoDataBacktesting

    class MyStrategy(Strategy):
        def initialize(self):
            # Use continuous futures for clean backtesting
            self.asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        def on_trading_iteration(self):
            # Get historical data
            bars = self.get_historical_prices(self.asset, 20, "minute")
            if bars and not bars.df.empty:
                # Your strategy logic here
                pass

    # Run backtest with DataBento
    if __name__ == "__main__":
        results = MyStrategy.backtest(
            DataBentoDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK)
        )

Supported Assets
================

DataBento supports a wide range of instruments:

**Stocks**

.. code-block:: python

    # Major stocks
    aapl = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    msft = Asset("MSFT", asset_type=Asset.AssetType.STOCK)
    googl = Asset("GOOGL", asset_type=Asset.AssetType.STOCK)

**Futures**

.. code-block:: python

    # Equity index futures (continuous)
    es = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)  # S&P 500
    nq = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)  # NASDAQ 100
    rty = Asset("RTY", asset_type=Asset.AssetType.CONT_FUTURE)  # Russell 2000
    
    # Micro futures
    mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)  # Micro S&P 500
    mnq = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)  # Micro NASDAQ 100
    m2k = Asset("M2K", asset_type=Asset.AssetType.CONT_FUTURE)  # Micro Russell 2000
    
    # Commodity futures
    cl = Asset("CL", asset_type=Asset.AssetType.CONT_FUTURE)   # Crude Oil
    gc = Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE)   # Gold
    ng = Asset("NG", asset_type=Asset.AssetType.CONT_FUTURE)   # Natural Gas

**Options** (when supported)

.. code-block:: python

    from datetime import date
    
    # Stock options
    aapl_call = Asset(
        symbol="AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2025, 12, 19),
        strike=150,
        right="CALL"
    )

Time Frames
===========

DataBento supports multiple timeframes:

.. code-block:: python

    class DataStrategy(Strategy):
        def on_trading_iteration(self):
            # Different timeframes
            minute_data = self.get_historical_prices(self.asset, 100, "minute")
            hour_data = self.get_historical_prices(self.asset, 24, "hour") 
            daily_data = self.get_historical_prices(self.asset, 30, "day")
            
            # Use the data for analysis
            if minute_data and not minute_data.df.empty:
                # High-frequency analysis
                latest_price = minute_data.df['close'].iloc[-1]

Advanced Configuration
=====================

You can configure DataBento backtesting with additional parameters:

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import DataBentoDataBacktesting

    # Custom backtest configuration
    backtest_start = datetime(2024, 1, 1)
    backtest_end = datetime(2024, 12, 31)

    results = MyStrategy.backtest(
        DataBentoDataBacktesting,
        start=backtest_start,
        end=backtest_end,
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        show_plot=True,
        show_tearsheet=True,
        save_tearsheet=True
    )

Data Quality Features
====================

DataBento provides several data quality features:

**Corporate Actions**
- Automatic dividend adjustments
- Stock split adjustments
- Merger and acquisition handling

**Data Cleaning**
- Outlier detection and removal
- Gap filling for missing data
- Timestamp normalization

**Market Hours**
- Proper market hour filtering
- Pre-market and after-hours data
- Holiday schedule handling

Caching
=======

Lumibot automatically caches DataBento data to improve performance:

.. code-block:: python

    # Data is automatically cached locally
    # Subsequent requests for the same data will be faster
    bars = self.get_historical_prices(asset, 100, "minute")

Cache files are stored in the Lumibot cache directory and are automatically managed.

Best Practices
==============

1. **Use Continuous Futures**
   
   For futures backtesting, always use continuous contracts for seamless data across expiration rollovers.

2. **Batch Data Requests**
   
   Request larger chunks of data rather than making many small requests.

3. **Monitor API Limits**
   
   DataBento has API rate limits. Avoid excessive requests in short time periods.

4. **Cache Management**
   
   Let Lumibot handle caching automatically. Clear cache only when needed.

5. **Data Validation**
   
   Always check that data is available before using it in your strategy.

Example: Multi-Asset Strategy
============================

Here's a complete example using multiple assets with DataBento:

.. code-block:: python

    from lumibot.strategies import Strategy
    from lumibot.entities import Asset, Order
    from lumibot.backtesting import DataBentoDataBacktesting
    import pandas as pd

    class MultiAssetStrategy(Strategy):
        def initialize(self):
            # Portfolio of futures contracts
            self.assets = [
                Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE),  # Micro S&P 500
                Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE),  # Micro NASDAQ 100
                Asset("M2K", asset_type=Asset.AssetType.CONT_FUTURE),  # Micro Russell 2000
            ]
            self.lookback_period = 20
            
        def on_trading_iteration(self):
            for asset in self.assets:
                # Get data for each asset
                bars = self.get_historical_prices(asset, self.lookback_period, "day")
                
                if bars and len(bars.df) >= self.lookback_period:
                    # Calculate momentum
                    returns = bars.df['close'].pct_change().dropna()
                    momentum = returns.tail(5).mean()  # 5-day average return
                    
                    position = self.get_position(asset)
                    
                    # Long momentum strategy
                    if momentum > 0.001:  # Positive momentum threshold
                        if position is None or position.quantity <= 0:
                            order = self.create_order(asset, 1, "buy")
                            self.submit_order(order)
                    
                    # Short momentum strategy  
                    elif momentum < -0.001:  # Negative momentum threshold
                        if position is None or position.quantity >= 0:
                            if position and position.quantity > 0:
                                # Close long first
                                close_order = self.create_order(asset, position.quantity, "sell")
                                self.submit_order(close_order)
                            # Then go short
                            order = self.create_order(asset, 1, "sell")
                            self.submit_order(order)

    if __name__ == "__main__":
        results = MultiAssetStrategy.backtest(
            DataBentoDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK)
        )

Error Handling
==============

Handle common DataBento issues gracefully:

.. code-block:: python

    class RobustStrategy(Strategy):
        def on_trading_iteration(self):
            try:
                bars = self.get_historical_prices(self.asset, 20, "minute")
                
                if bars is None or bars.df.empty:
                    self.log_message("No data available", color="yellow")
                    return
                
                # Your strategy logic here
                
            except Exception as e:
                self.log_message(f"Data error: {e}", color="red")
                return

Performance Optimization
=======================

Tips for optimizing DataBento performance:

1. **Minimize Data Requests**
   
   Request data once and reuse it within the same iteration.

2. **Use Appropriate Timeframes**
   
   Don't request minute data if you only need daily signals.

3. **Leverage Caching**
   
   Repeated backtests will be faster due to automatic caching.

4. **Batch Processing**
   
   Process multiple assets efficiently in loops.

Troubleshooting
==============

**Common Issues:**

1. **"No DataBento API key found"**
   
   - Set the ``DATABENTO_API_KEY`` environment variable
   - Check your .env file configuration

2. **"Rate limit exceeded"**
   
   - Reduce the frequency of data requests
   - Use longer timeframes when possible
   - Add delays between requests if needed

3. **"No data available for symbol"**
   
   - Verify the symbol is correct
   - Check if DataBento supports the instrument
   - Ensure the date range is valid

4. **"Connection timeout"**
   
   - Check your internet connection
   - Verify DataBento service status
   - Retry the request

Cost Considerations
==================

DataBento is a premium service with costs based on:

- **Data volume** (number of symbols and timeframes)
- **Historical depth** (how far back you request data)
- **API usage** (number of requests)

For cost-effective backtesting:

- Use continuous futures instead of multiple expiry contracts
- Request appropriate timeframes (don't use minute data for daily strategies)
- Leverage caching to avoid repeated requests
- Focus on the symbols you actually need

DataBento provides excellent value for professional strategy development due to its data quality and reliability.
