Futures Trading
***************

Lumibot provides robust support for futures trading, offering both specific expiry futures and continuous futures contracts. This guide covers how to create futures assets, understand the different types, and best practices for backtesting and live trading.

Types of Futures Assets
=======================

Lumibot supports three types of futures contracts:

1. **Specific Expiry Futures** - For live trading with exact expiration dates
2. **Auto-Expiry Futures** - Automatically select front month contracts
3. **Continuous Futures** - Seamless contracts for backtesting (recommended)

Continuous Futures (Recommended for Backtesting)
================================================

Continuous futures are the simplest and most reliable approach for backtesting. They eliminate the complexity of managing expiration dates and contract rollovers.

.. code-block:: python

    from lumibot.entities import Asset
    from lumibot.strategies import Strategy
    
    class MyFuturesStrategy(Strategy):
        def initialize(self):
            # Continuous futures - no expiration to manage
            self.mes_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
            self.es_asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
            self.nq_asset = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)

**Benefits of Continuous Futures:**

- No expiration date management
- Seamless backtesting across multiple years
- No contract rollover complexity
- Supported by most data providers
- Ideal for strategy development and testing

Specific Expiry Futures
=======================

For live trading or when you need to trade specific contract months, you can specify exact expiration dates:

.. code-block:: python

    from datetime import date
    from lumibot.entities import Asset
    
    # Specific expiry future
    asset = Asset(
        symbol="ES",
        asset_type=Asset.AssetType.FUTURE,
        expiration=date(2025, 12, 20)  # December 2025 expiry
    )

Auto-Expiry Futures
===================

Auto-expiry futures automatically calculate the appropriate expiration date based on the current date. This is useful for live trading when you always want the front month contract.

.. code-block:: python

    from lumibot.entities import Asset
    
    # Auto-expiry futures (front month)
    asset = Asset(
        symbol="MES",
        asset_type=Asset.AssetType.FUTURE,
        auto_expiry=Asset.AutoExpiry.FRONT_MONTH
    )
    
    # Auto-expiry futures (next quarter)
    asset = Asset(
        symbol="ES",
        asset_type=Asset.AssetType.FUTURE,
        auto_expiry=Asset.AutoExpiry.NEXT_QUARTER
    )

**Auto-Expiry Options:**

- ``Asset.AutoExpiry.FRONT_MONTH`` - Always use the nearest quarterly expiry
- ``Asset.AutoExpiry.NEXT_QUARTER`` - Use the next quarterly expiry (same as front month for most contracts)
- ``Asset.AutoExpiry.AUTO`` - Default to front month behavior

Popular Futures Symbols
=======================

Here are some commonly traded futures contracts:

**Equity Index Futures:**

.. code-block:: python

    # E-mini S&P 500
    es_asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
    
    # Micro E-mini S&P 500
    mes_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
    
    # E-mini NASDAQ 100
    nq_asset = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)
    
    # Micro E-mini NASDAQ 100
    mnq_asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
    
    # E-mini Russell 2000
    rty_asset = Asset("RTY", asset_type=Asset.AssetType.CONT_FUTURE)
    
    # Micro E-mini Russell 2000
    m2k_asset = Asset("M2K", asset_type=Asset.AssetType.CONT_FUTURE)

**Energy Futures:**

.. code-block:: python

    # Crude Oil
    cl_asset = Asset("CL", asset_type=Asset.AssetType.CONT_FUTURE)
    
    # Natural Gas
    ng_asset = Asset("NG", asset_type=Asset.AssetType.CONT_FUTURE)

**Metal Futures:**

.. code-block:: python

    # Gold
    gc_asset = Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE)
    
    # Silver
    si_asset = Asset("SI", asset_type=Asset.AssetType.CONT_FUTURE)

Complete Strategy Example
========================

Here's a complete example of a futures trading strategy using continuous futures:

.. code-block:: python

    from lumibot.entities import Asset, Order
    from lumibot.strategies import Strategy
    
    class SimpleFuturesStrategy(Strategy):
        def initialize(self):
            # Use continuous futures for clean backtesting
            self.asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
            self.order_size = 10
            
            # Log which asset we're trading
            self.log_message(f"Trading {self.asset.symbol} continuous futures")
        
        def on_trading_iteration(self):
            # Get current price
            current_price = self.get_last_price(self.asset)
            
            # Simple moving average strategy
            bars = self.get_historical_prices(self.asset, 20, "day")
            if bars and len(bars.df) >= 20:
                sma_20 = bars.df['close'].rolling(20).mean().iloc[-1]
                
                position = self.get_position(self.asset)
                
                # Buy signal: price above SMA
                if current_price > sma_20 and (position is None or position.quantity <= 0):
                    if position and position.quantity < 0:
                        # Cover short position
                        self.create_order(self.asset, abs(position.quantity), "buy")
                    # Go long
                    order = self.create_order(self.asset, self.order_size, "buy")
                    self.submit_order(order)
                
                # Sell signal: price below SMA
                elif current_price < sma_20 and (position is None or position.quantity >= 0):
                    if position and position.quantity > 0:
                        # Close long position
                        self.create_order(self.asset, position.quantity, "sell")
                    # Go short
                    order = self.create_order(self.asset, self.order_size, "sell")
                    self.submit_order(order)

Best Practices
==============

1. **Use Continuous Futures for Backtesting**
   
   Continuous futures eliminate expiration complexity and provide cleaner backtests.

2. **Use Auto-Expiry for Live Trading**
   
   When live trading, auto-expiry ensures you're always trading the most liquid front month contract.

3. **Consider Contract Size**
   
   Micro contracts (MES, MNQ, M2K) require less capital than full-size contracts (ES, NQ, RTY).

4. **Monitor Margin Requirements**
   
   Futures require margin, which varies by contract and broker. Always check your margin requirements.

5. **Handle Trading Hours**
   
   Futures trade nearly 24 hours. Be aware of market hours and liquidity patterns.

6. **Risk Management**
   
   Futures use leverage, so implement proper risk management with stop losses and position sizing.

Example with Risk Management
===========================

.. code-block:: python

    class RiskManagedFuturesStrategy(Strategy):
        def initialize(self):
            self.asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
            self.max_position_size = 5
            self.stop_loss_pct = 0.02  # 2% stop loss
            
        def on_trading_iteration(self):
            current_price = self.get_last_price(self.asset)
            position = self.get_position(self.asset)
            
            # Risk management: check for stop loss
            if position and position.quantity != 0:
                unrealized_pnl_pct = position.quantity * (current_price - position.avg_fill_price) / position.avg_fill_price
                
                if abs(unrealized_pnl_pct) > self.stop_loss_pct:
                    # Exit position if stop loss hit
                    if position.quantity > 0:
                        order = self.create_order(self.asset, position.quantity, "sell")
                    else:
                        order = self.create_order(self.asset, abs(position.quantity), "buy")
                    self.submit_order(order)
                    return
            
            # Your trading logic here...

Common Issues and Solutions
==========================

**Issue: "No data available for futures contract"**

- Solution: Make sure your data provider supports the futures symbol
- Use continuous futures for backtesting
- Check that the symbol is correct (e.g., "ES" not "SPX")

**Issue: "Contract expired"**

- Solution: Use continuous futures for backtesting or auto-expiry for live trading
- Manually specify future expiration dates when needed

**Issue: "Insufficient margin"**

- Solution: Reduce position size or use micro contracts
- Check your broker's margin requirements
- Ensure adequate account funding

**Issue: "Low liquidity outside market hours"**

- Solution: Trade during regular market hours for best execution
- Use limit orders instead of market orders during off-hours
- Consider using more liquid contracts (ES vs RTY)

Data Provider Support
====================

Different data providers have varying levels of futures support:

- **DataBento**: Excellent futures support with continuous and specific expiry data
- **Polygon**: Good support for major futures contracts
- **Interactive Brokers**: Full futures support including margin calculations
- **Yahoo**: Limited futures data (mostly indices)

For comprehensive futures backtesting, DataBento is recommended due to its clean data and extensive contract coverage.
