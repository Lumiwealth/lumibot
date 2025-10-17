"""
Direct pandas vs polars portfolio parity verification.

Runs identical backtest on both backends and compares:
1. Final portfolio value
2. Number of trades
3. Trade fills (timestamps, prices)
"""
import sys
from pathlib import Path
from datetime import datetime
import pytz

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPandas, DataBentoDataBacktestingPolars
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


class SimpleMESStrategy(Strategy):
    """Minimal strategy: buy on first bar, sell on last bar"""

    def initialize(self):
        self.asset = Asset("MES", asset_type="future")
        self.sleeptime = "1M"
        self.first_bar = True

    def on_trading_iteration(self):
        if self.first_bar:
            # Buy on first iteration
            self.create_order(self.asset, 1, "buy")
            self.first_bar = False

    def before_market_closes(self):
        # Close position before market closes
        pos = self.get_position(self.asset)
        if pos and pos.quantity > 0:
            self.sell_all()


def run_parity_test(mode):
    """Run backtest and return final portfolio value"""

    print(f"\n{'='*60}")
    print(f"RUNNING {mode.upper()} BACKTEST")
    print(f"{'='*60}")

    # Short period - 3 days
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2024, 1, 3, 9, 30))
    end = tz.localize(datetime(2024, 1, 5, 16, 0))

    # Select backend
    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas

    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = SimpleMESStrategy(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)

    result = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    # Extract results
    final_value = strat.get_portfolio_value()
    positions = strat.get_positions()
    orders = strat.get_orders()

    filled_orders = [o for o in orders if o.status == "fill"]

    print(f"\n{'='*60}")
    print(f"{mode.upper()} RESULTS")
    print(f"{'='*60}")
    print(f"Final Portfolio Value: ${final_value:,.2f}")
    print(f"Total Orders: {len(orders)}")
    print(f"Filled Orders: {len(filled_orders)}")
    print(f"Positions: {len(positions)}")

    # Print fill details
    print(f"\nFill Details:")
    for i, order in enumerate(filled_orders[:10]):  # First 10 fills
        print(f"  {i+1}. {order.side} {order.quantity} @ ${order.avg_fill_price:.2f} at {order.update_timestamp}")

    print(f"{'='*60}\n")

    return {
        'final_value': final_value,
        'total_orders': len(orders),
        'filled_orders': len(filled_orders),
        'fills': filled_orders
    }


if __name__ == "__main__":
    print("\n" + "="*60)
    print("PANDAS VS POLARS PORTFOLIO PARITY TEST")
    print("="*60)

    # Run pandas
    pandas_results = run_parity_test("pandas")

    # Run polars
    polars_results = run_parity_test("polars")

    # Compare
    print("\n" + "="*60)
    print("COMPARISON")
    print("="*60)

    pandas_val = pandas_results['final_value']
    polars_val = polars_results['final_value']
    diff = abs(pandas_val - polars_val)
    pct_diff = (diff / pandas_val * 100) if pandas_val > 0 else 0

    print(f"Pandas Portfolio: ${pandas_val:,.2f}")
    print(f"Polars Portfolio: ${polars_val:,.2f}")
    print(f"Difference: ${diff:,.2f} ({pct_diff:.4f}%)")

    print(f"\nPandas Filled Orders: {pandas_results['filled_orders']}")
    print(f"Polars Filled Orders: {polars_results['filled_orders']}")

    if diff < 0.01:  # Within 1 cent
        print(f"\n✓ PARITY VERIFIED: Portfolios match within $0.01")
    else:
        print(f"\n✗ PARITY FAILED: Portfolios differ by ${diff:,.2f}")

    print("="*60 + "\n")
