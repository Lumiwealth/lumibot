"""
Deep dive comparison: ThetaData vs Polygon
Logs every detail to understand divergence
"""
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Enable detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/thetadata_debug.log'),
        logging.StreamHandler()
    ]
)

# Set environment
os.environ['BACKTESTING_START'] = '2025-09-01'
os.environ['BACKTESTING_END'] = '2025-09-05'
os.environ['BACKTESTING_QUIET_LOGS'] = 'False'
os.environ['BACKTESTING_SHOW_PROGRESS_BAR'] = 'True'

sys.path.insert(0, '/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot')

from lumibot.strategies import Strategy
from lumibot.entities import Asset
from lumibot.backtesting import ThetaDataBacktesting, PolygonDataBacktesting
from dotenv import load_dotenv

load_dotenv('/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/.env.test')

# Import the strategy
exec(open('/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/PLTR Weekly Call Roller.py').read())

print("=" * 100)
print("DETAILED THETADATA BACKTEST")
print("=" * 100)

os.environ['BACKTESTING_DATA_SOURCE'] = 'ThetaData'
theta_results = PLTRWeeklyCallRoller.backtest(
    ThetaDataBacktesting,
    backtesting_start=datetime(2025, 9, 1),
    backtesting_end=datetime(2025, 9, 5),
    benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
    quote_asset=Asset("USD", Asset.AssetType.FOREX),
)

print("\n" + "=" * 100)
print("DETAILED POLYGON BACKTEST")
print("=" * 100)

os.environ['BACKTESTING_DATA_SOURCE'] = 'Polygon'
polygon_results = PLTRWeeklyCallRoller.backtest(
    PolygonDataBacktesting,
    backtesting_start=datetime(2025, 9, 1),
    backtesting_end=datetime(2025, 9, 5),
    benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
    quote_asset=Asset("USD", Asset.AssetType.FOREX),
)

print("\n" + "=" * 100)
print("COMPARISON ANALYSIS")
print("=" * 100)

# Portfolio values
theta_final = theta_results['portfolio_value'][-1]
polygon_final = polygon_results['portfolio_value'][-1]

print(f"\nFinal Portfolio Values:")
print(f"  ThetaData: ${theta_final:,.2f}")
print(f"  Polygon:   ${polygon_final:,.2f}")
print(f"  Difference: ${abs(theta_final - polygon_final):,.2f}")

# Returns
theta_return = (theta_final - 100000) / 100000 * 100
polygon_return = (polygon_final - 100000) / 100000 * 100
print(f"\nReturns:")
print(f"  ThetaData: {theta_return:.2f}%")
print(f"  Polygon:   {polygon_return:.2f}%")

# Analyze trades from logs
print(f"\nLog file created at: /tmp/thetadata_debug.log")
print("Search log for:")
print("  - 'Submitting order' to see trade submissions")
print("  - 'Fill price' to see execution prices")
print("  - 'bid' and 'ask' to see quote data")
print("  - 'get_last_price' to see price lookups")
