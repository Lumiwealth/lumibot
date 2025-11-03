"""
Gold (GC) Futures Trading Strategy - Optimized with DataBento

This example demonstrates how to use DataBento as a data source for Gold futures trading
with Lumibot. It shows how to:
1. Configure DataBento as a data source with prefetch optimization
2. Create a simple Gold futures trading strategy using continuous contracts
3. Backtest using DataBento data with minimal code changes

Features:
- Continuous futures (no expiration management needed)
- Prefetch optimization for 5.6x faster backtesting
- Moving average crossover system
- Automatic API key loading from environment

Requirements:
- DataBento API key (set in .env file as DATABENTO_API_KEY)
- databento Python package: pip install databento
"""

import os
from datetime import datetime, timedelta
import pandas as pd
from lumibot.strategies import Strategy
from lumibot.entities import Asset
from lumibot.backtesting import DataBentoDataBacktesting

# =============================================================================
# DIAGNOSTICS TOGGLE
# =============================================================================
PLOT_PRICE = True  # Set to True to plot close prices after prefetch for data validation
PLOT_SAVE_PATH = "logs/gc_futures_price_data.png"  # Where to save the plot

# Live Equity Curve GUI
SHOW_LIVE_EQUITY_CURVE = True  # Set to True to show live-updating equity curve during backtest

# =============================================================================


class GCFuturesOptimized(Strategy):
    """
    Optimized strategy using DataBento for Gold (GC) futures data

    This strategy implements a simple moving average crossover system for COMEX Gold futures.
    Uses continuous futures contracts for seamless backtesting without expiration management.

    Contract Specifications:
    - Symbol: GC (COMEX Gold)
    - Contract Size: 100 troy ounces
    - Tick Size: $0.10/oz = $10/contract
    - Margin: ~$10,000 (simulated in backtesting)
    - Trading Hours: Nearly 24-hour (Sun-Fri)
    """

    def initialize(self):
        """Initialize the strategy"""
        # CRITICAL: Set market calendar for futures trading hours
        # Gold (GC) trades on CME: Sun 6pm - Fri 5pm ET with ~1hr daily maintenance break
        # NOT 24/7 - weekends are closed, and TopStepX may have more restrictive hours
        # TODO: Verify exact CME calendar or create custom calendar for TopStepX
        self.set_market("us_futures")  # CME futures calendar (proper for GC)
        self.log_message(f"✅ Market calendar set to: us_futures (CME calendar)")

        # Set the sleep time between iterations
        # NOTE: Due to a lumibot bug, integer sleeptime is multiplied by 60 (treated as minutes)
        # Use string format to specify seconds explicitly: "300S" = 300 seconds = 5 minutes
        self.sleeptime = "300S"  # 300 seconds = 5 minutes

        # Define the futures contract we want to trade
        # Using continuous Gold futures (no expiration needed)
        self.asset = Asset(
            symbol="GC",
            asset_type="cont_future"  # Continuous futures for seamless backtesting
        )

        # Prefetch optimization for faster backtesting
        # This loads all required data upfront, reducing API calls from 2500+ to ~5
        if hasattr(self._data_source, 'initialize_data_for_backtest'):
            self._data_source.initialize_data_for_backtest(
                strategy_assets=[self.asset],
                timestep="minute"  # MUST use minute-level data for intraday strategy
            )
            self.log_message("✅ Data prefetch complete - optimization enabled")

            # Diagnostic: Plot price data if enabled
            if PLOT_PRICE:
                self._plot_prefetched_data()

        # Moving average periods
        self.short_ma_period = 10
        self.long_ma_period = 30

        # Position sizing
        self.position_size = 1  # Number of contracts

        # Track last signal to avoid over-trading
        self.last_signal = None

        self.log_message("Gold (GC) Futures Strategy initialized")
        self.log_message(f"Trading asset: {self.asset.symbol} continuous futures")
        
        # Track statistics for diagnostics
        self.iteration_count = 0
        self.insufficient_data_count = 0
        self.signal_count = 0

        # Diagnostic: Track iteration timestamps for debugging
        self.iteration_timestamps = []
        self.iteration_log_file = "logs/gc_iteration_log.txt"

        # Clear previous log and write header
        os.makedirs("logs", exist_ok=True)
        with open(self.iteration_log_file, 'w') as f:
            f.write("=== GC Futures Iteration Log ===\n")
            f.write(f"Strategy start: {self.get_datetime()}\n")
            f.write(f"Market setting: {self.broker.market if hasattr(self, 'broker') else 'N/A'}\n")
            f.write(f"Data source: {self._data_source.__class__.__name__}\n")
            f.write(f"Sleeptime: {self.sleeptime} seconds\n\n")
            f.write("Iteration Log:\n")

        # For deferred plotting: collect 1-minute bars during backtest
        self._bars_dataframes = []  # List of DataFrames with 1-minute bars

    def _plot_prefetched_data(self):
        """Plot the prefetched price data for diagnostic purposes"""
        try:
            import matplotlib
            matplotlib.use('Agg')  # Non-interactive backend
            import matplotlib.pyplot as plt
        except ImportError:
            self.log_message("⚠️ matplotlib not available - cannot plot price data")
            return
        
        try:
            # Try multiple ways to access the prefetched data
            quote_asset = Asset("USD", "forex")
            search_key = (self.asset, quote_asset)
            
            df = None
            data_source = self._data_source
            
            # Method 1: Try pandas_data
            if hasattr(data_source, 'pandas_data'):
                if search_key in data_source.pandas_data:
                    data_obj = data_source.pandas_data[search_key]
                    if hasattr(data_obj, 'df'):
                        df = data_obj.df
                        self.log_message(f"✅ Found data in pandas_data: {len(df) if df is not None and not df.empty else 0} bars")
            
            # Method 2: Try _data_store
            if (df is None or df.empty) and hasattr(data_source, '_data_store'):
                if search_key in data_source._data_store:
                    data_obj = data_source._data_store[search_key]
                    if hasattr(data_obj, 'df'):
                        df = data_obj.df
                        self.log_message(f"✅ Found data in _data_store: {len(df) if df is not None and not df.empty else 0} bars")
            
            # Method 3: Try to get data by requesting a large historical window
            if df is None or df.empty:
                self.log_message("⚠️ Data not found in prefetched storage, attempting to fetch directly...")
                try:
                    # Try to get all data for the backtest period
                    bars = self.get_historical_prices(
                        asset=self.asset,
                        length=1000000,  # Very large number to get all available data
                        timestep="minute"
                    )
                    if bars is not None and hasattr(bars, 'df') and not bars.df.empty:
                        df = bars.df
                        self.log_message(f"✅ Retrieved data directly: {len(df)} bars")
                except Exception as e:
                    self.log_message(f"⚠️ Could not fetch data directly: {e}")
            
            # If we still don't have data, log what's available
            if df is None or df.empty:
                self.log_message("⚠️ WARNING: Could not find or access prefetched data for plotting!")
                if hasattr(data_source, 'pandas_data'):
                    keys = list(data_source.pandas_data.keys()) if data_source.pandas_data else []
                    self.log_message(f"   Available keys in pandas_data: {len(keys)} keys")
                    for key in keys[:5]:  # Show first 5 keys
                        self.log_message(f"     - {key}")
                if hasattr(data_source, '_data_store'):
                    keys = list(data_source._data_store.keys()) if data_source._data_store else []
                    self.log_message(f"   Available keys in _data_store: {len(keys)} keys")
                return
            
            # Create the plot
            if df is not None and not df.empty:
                plt.figure(figsize=(16, 8))
                plt.plot(df.index, df['close'], linewidth=2.0, color='black')
                plt.title(f'GC Futures Close Prices (1-minute bars)\n'
                         f'Date Range: {df.index[0]} to {df.index[-1]}\n'
                         f'Total Bars: {len(df)}', fontsize=14)
                plt.xlabel('Time', fontsize=12)
                plt.ylabel('Price ($)', fontsize=12)
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                
                # Save the plot
                os.makedirs(os.path.dirname(PLOT_SAVE_PATH), exist_ok=True)
                plt.savefig(PLOT_SAVE_PATH, dpi=150, bbox_inches='tight')
                plt.close()
                
                self.log_message(f"📊 Price plot saved to: {PLOT_SAVE_PATH}")
                self.log_message(f"📈 Data summary: {len(df)} bars from {df.index[0]} to {df.index[-1]}")
                self.log_message(f"📈 Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
                
                # Additional diagnostic: check for gaps in data
                time_diff = df.index.to_series().diff()
                expected_diff = pd.Timedelta(minutes=1)
                gaps = time_diff[time_diff > expected_diff * 2]  # Gaps > 2 minutes
                if len(gaps) > 0:
                    self.log_message(f"⚠️ Found {len(gaps)} data gaps (intervals > 2 minutes)")
                    self.log_message(f"   Largest gap: {gaps.max()} at {gaps.idxmax()}")
                
        except Exception as e:
            self.log_message(f"⚠️ Error plotting price data: {e}")
            import traceback
            self.log_message(f"   Traceback: {traceback.format_exc()}")

    def on_trading_iteration(self):
        """Main trading logic executed on each iteration"""

        current_time = self.get_datetime()
        self.iteration_count += 1
        self.iteration_timestamps.append(current_time)

        # Log every iteration to file for detailed analysis
        with open(self.iteration_log_file, 'a') as f:
            f.write(f"{self.iteration_count}: {current_time}\n")

        # Log summary every 100 iterations to console
        if self.iteration_count % 100 == 0:
            self.log_message(f"📊 Iteration {self.iteration_count} at {current_time}")

        # Get historical price data for moving averages
        # Requesting 40 1-MINUTE bars (not "40-minute bars")
        bars = self.get_historical_prices(
            asset=self.asset,
            length=self.long_ma_period + 10,  # 40 bars total (30 + 10 buffer)
            timestep="minute"  # 1-minute bars as originally intended
        )

        if bars is None or len(bars.df) < self.long_ma_period:
            self.insufficient_data_count += 1
            current_time = self.get_datetime()
            data_length = len(bars.df) if bars is not None else 0
            # Log every 100th occurrence to avoid spam
            if self.insufficient_data_count % 100 == 1 or self.insufficient_data_count <= 5:
                self.log_message(f"⚠️ Insufficient data at {current_time}: "
                               f"got {data_length} bars, need {self.long_ma_period} bars")
                # Also log to iteration file for debugging
                with open(self.iteration_log_file, 'a') as f:
                    f.write(f"  └─ INSUFFICIENT DATA: got {data_length} bars, need {self.long_ma_period}\n")
            # Log summary every 500 iterations
            if self.iteration_count % 500 == 0:
                self.log_message(f"📊 Diagnostics: {self.iteration_count} iterations, "
                               f"{self.insufficient_data_count} insufficient data, "
                               f"{self.signal_count} signals generated")
            return

        # Calculate moving averages
        df = bars.df
        short_ma = df['close'].rolling(window=self.short_ma_period).mean()
        long_ma = df['close'].rolling(window=self.long_ma_period).mean()

        # Get current values
        current_short_ma = short_ma.iloc[-1]
        current_long_ma = long_ma.iloc[-1]
        current_price = df['close'].iloc[-1]
        
        # Collect all 1-minute bars for deferred plotting (collect full bars dataframe each iteration)
        if PLOT_PRICE:
            # Store the full bars dataframe to ensure 1-minute granularity
            if not hasattr(self, '_bars_dataframes'):
                self._bars_dataframes = []
            # Append the current bars dataframe to collect all 1-minute data
            if bars is not None and hasattr(bars, 'df') and not bars.df.empty:
                self._bars_dataframes.append(bars.df.copy())

        # Get previous values for crossover detection
        prev_short_ma = short_ma.iloc[-2]
        prev_long_ma = long_ma.iloc[-2]

        # Determine signal
        signal = None

        # Bullish crossover: short MA crosses above long MA
        if prev_short_ma <= prev_long_ma and current_short_ma > current_long_ma:
            signal = "BUY"
            self.signal_count += 1

        # Bearish crossover: short MA crosses below long MA
        elif prev_short_ma >= prev_long_ma and current_short_ma < current_long_ma:
            signal = "SELL"
            self.signal_count += 1

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
            # For Gold futures: each point = $100 (100 oz contract)
            unrealized_pnl = current_position.quantity * (current_price - current_position.avg_fill_price) * 100
            self.log_message(f"Current position: {current_position.quantity} contracts, "
                           f"Avg price: {current_position.avg_fill_price:.2f}, "
                           f"Unrealized P&L: ${unrealized_pnl:.2f}")
    
    def on_strategy_end(self):
        """Called when backtesting ends - log diagnostic summary"""
        self.log_message("="*80)
        self.log_message("📊 BACKTEST DIAGNOSTICS SUMMARY")
        self.log_message("="*80)
        self.log_message(f"Total iterations: {self.iteration_count}")

        # Calculate iteration frequency
        if len(self.iteration_timestamps) > 1:
            from datetime import timedelta
            time_diffs = [(self.iteration_timestamps[i] - self.iteration_timestamps[i-1]).total_seconds() / 60.0
                          for i in range(1, min(len(self.iteration_timestamps), 100))]  # Sample first 100
            avg_minutes_between = sum(time_diffs) / len(time_diffs) if time_diffs else 0

            # Calculate expected iterations
            total_days = 304  # Jan 1 - Oct 31 = ~304 days
            expected_24_7 = (total_days * 24 * 60) / 5  # 5-minute intervals, 24/7
            expected_nyse = total_days * 78  # 6.5 hours * 12 intervals per hour
            iterations_per_day = self.iteration_count / total_days

            self.log_message(f"Average time between iterations: {avg_minutes_between:.1f} minutes")
            self.log_message(f"Iterations per day: {iterations_per_day:.1f}")
            self.log_message(f"Expected if 24/7 market (5min intervals): {expected_24_7:.0f} iterations ({expected_24_7/total_days:.1f}/day)")
            self.log_message(f"Expected if NYSE hours (5min intervals): {expected_nyse:.0f} iterations ({expected_nyse/total_days:.1f}/day)")

            if iterations_per_day < 20:
                self.log_message("⚠️ WARNING: Very few iterations per day - likely using stock market hours!")
            elif iterations_per_day > 250:
                self.log_message("✅ High iteration count - likely using 24/7 or futures market hours")

        if self.iteration_count > 0:
            insufficient_pct = 100 * self.insufficient_data_count / self.iteration_count
            self.log_message(f"Iterations with insufficient data: {self.insufficient_data_count} "
                            f"({insufficient_pct:.1f}%)")
        else:
            self.log_message(f"Iterations with insufficient data: {self.insufficient_data_count}")

        self.log_message(f"Total crossover signals generated: {self.signal_count}")

        # Write summary to iteration log
        with open(self.iteration_log_file, 'a') as f:
            f.write(f"\n=== SUMMARY ===\n")
            f.write(f"Total iterations: {self.iteration_count}\n")
            f.write(f"Insufficient data: {self.insufficient_data_count}\n")
            f.write(f"Signals generated: {self.signal_count}\n")
            if len(self.iteration_timestamps) > 1:
                f.write(f"Avg minutes between iterations: {avg_minutes_between:.1f}\n")
                f.write(f"Iterations per day: {iterations_per_day:.1f}\n")

        self.log_message(f"Iteration log saved to: {self.iteration_log_file}")
        
        # Try to plot from collected bars if prefetch plot didn't work
        if PLOT_PRICE and len(self._bars_dataframes) > 0:
            self._plot_from_samples()
        
        if self.iteration_count > 0 and self.insufficient_data_count > self.iteration_count * 0.5:
            self.log_message("⚠️ WARNING: More than 50% of iterations had insufficient data!")
            self.log_message("   This suggests potential data quality or availability issues.")
            self.log_message("   Check the price plot if PLOT_PRICE=True to visualize data gaps.")
        
        if self.signal_count < 10:
            self.log_message("⚠️ WARNING: Very few signals generated for the backtest period!")
            self.log_message("   This could indicate:")
            self.log_message("   1. Data gaps or missing bars")
            self.log_message("   2. Moving averages not crossing frequently enough")
            self.log_message("   3. Insufficient data causing most iterations to be skipped")
        
        self.log_message("="*80)
    
    def _plot_from_samples(self):
        """Plot price data from collected 1-minute bars during backtest"""
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            
            if len(self._bars_dataframes) == 0:
                return
            
            # Combine all collected dataframes and remove duplicates (keep latest)
            # This ensures we get all 1-minute bars at full resolution
            df_combined = pd.concat(self._bars_dataframes, ignore_index=False)
            # Remove duplicates by index (datetime), keeping the last occurrence
            df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
            df_combined = df_combined.sort_index()
            
            if len(df_combined) < 2:
                self.log_message("⚠️ Not enough data collected for plotting")
                return
            
            # Create the plot with 1-minute granularity
            plt.figure(figsize=(16, 8))
            plt.plot(df_combined.index, df_combined['close'], linewidth=2.0, color='black')
            plt.title(f'GC Futures Close Prices (1-minute bars from backtest)\n'
                     f'Date Range: {df_combined.index[0]} to {df_combined.index[-1]}\n'
                     f'Total Bars: {len(df_combined)}', fontsize=14)
            plt.xlabel('Time', fontsize=12)
            plt.ylabel('Price ($)', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            # Save with different filename
            sample_plot_path = PLOT_SAVE_PATH.replace('.png', '_samples.png')
            os.makedirs(os.path.dirname(sample_plot_path), exist_ok=True)
            plt.savefig(sample_plot_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            self.log_message(f"📊 Price plot from collected bars saved to: {sample_plot_path}")
            self.log_message(f"📈 Total 1-minute bars plotted: {len(df_combined)}")
            self.log_message(f"📈 Price range: ${df_combined['close'].min():.2f} - ${df_combined['close'].max():.2f}")
            
        except Exception as e:
            self.log_message(f"⚠️ Error plotting from collected bars: {e}")
            import traceback
            self.log_message(f"   Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    """
    Backtest the Gold futures strategy using DataBento data

    Before running this, make sure to:
    1. Install databento: pip install databento
    2. Set your DataBento API key in .env file:
       DATABENTO_API_KEY=your_api_key_here
    3. Run from project root: python examples/gc_futures_optimized.py

    Expected Performance (with prefetch optimization):
    - Backtest time: ~8 minutes (vs 45 min without optimization)
    - API calls: ~5 (vs 2,500+ without optimization)
    - Log lines: ~50 (vs 15,000+ without optimization)
    """

    # Define backtest parameters for 2025
    # Testing with 1 month to verify fixes (change to full period when ready)
    backtest_start = datetime(2024, 1, 1)
    backtest_end = datetime(2025, 10, 31)  # January only for verification

    # Full backtest period (will take ~7+ hours with 24/7 calendar and 5-min intervals):
    # backtest_start = datetime(2025, 1, 1)
    # backtest_end = datetime(2025, 10, 31)

    # Load DataBento API key from environment (.env file)
    api_key = os.getenv("DATABENTO_API_KEY")

    if not api_key:
        print("❌ ERROR: DATABENTO_API_KEY not found in environment")
        print("Please set it in your .env file or export it:")
        print("  export DATABENTO_API_KEY='your_api_key_here'")
        exit(1)

    print("="*80)
    print("🥇 Gold (GC) Futures Backtesting Strategy")
    print("="*80)
    print(f"Period: {backtest_start.date()} to {backtest_end.date()}")
    print(f"Contract: GC continuous futures (COMEX Gold)")
    print(f"Strategy: Moving Average Crossover (10/30 periods)")
    print(f"Data Source: DataBento (optimized with prefetch)")
    print(f"API Key: {'✅ Loaded from .env' if api_key else '❌ Not found'}")
    print("="*80)
    print()

    # Set up backtesting with DataBento data source
    print("📊 Starting backtest...")
    print("⏱️  With prefetch optimization, this should take ~8 minutes")
    print()

    # Launch live equity curve GUI if enabled
    gui_process = None
    if SHOW_LIVE_EQUITY_CURVE:
        import subprocess
        import sys
        from pathlib import Path

        equity_file = Path("logs/backtest_live_equity.jsonl")

        # Clear existing equity file
        if equity_file.exists():
            equity_file.unlink()
            print("✅ Cleared existing equity curve data")

        # Launch GUI in separate process
        try:
            gui_process = subprocess.Popen(
                [sys.executable, "show_live_backtest_progress.py"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            print("📈 Launched live equity curve GUI")
            print(f"   GUI PID: {gui_process.pid}")
            print()
        except Exception as e:
            print(f"⚠️  Failed to launch GUI: {e}")
            print()

    # Run backtest using run_backtest method (matches lumibot pattern)
    results, strategy = GCFuturesOptimized.run_backtest(
        datasource_class=DataBentoDataBacktesting,
        backtesting_start=backtest_start,
        backtesting_end=backtest_end,
        api_key=api_key,
        write_live_equity_file=SHOW_LIVE_EQUITY_CURVE,  # Enable equity file writing
        show_plot=True,
        show_tearsheet=True,
        save_tearsheet=True,
        show_progress_bar=True
    )

    print()
    print("="*80)
    print("✅ Backtest Complete!")
    print("="*80)
    print("Check the output for:")
    print("  - Performance tearsheet (PDF)")
    print("  - Equity curve plot (PNG)")
    print("  - Trade statistics")
    if SHOW_LIVE_EQUITY_CURVE:
        print()
        print("📈 Live Equity Curve GUI:")
        print("  - GUI is still running in separate window")
        print("  - Close the GUI window when you're done reviewing")
        print("  - Or re-launch anytime: python3 show_live_backtest_progress.py")
    print("="*80)
