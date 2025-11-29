"""
ULTRA SIMPLE futures test - checking ONE thing at a time.

No complex strategies, no indicators, no bracket orders.
Just: Buy 1 contract → hold → sell → verify numbers match reality.
"""
import datetime
import pytest
import pytz

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars as DataBentoDataPolarsBacktesting
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


class UltraSimpleStrategy(Strategy):
    """Buy on iteration 1, sell on iteration 5. That's it."""

    def initialize(self):
        self.sleeptime = "30M"  # Every 30 minutes
        self.set_market("us_futures")
        self.mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        self.iteration = 0
        self.snapshots = []

    def on_trading_iteration(self):
        self.iteration += 1

        # Get state
        price = self.get_last_price(self.mes)
        cash = self.get_cash()
        portfolio = self.get_portfolio_value()
        position = self.get_position(self.mes)

        # Save snapshot
        self.snapshots.append({
            "iteration": self.iteration,
            "datetime": self.get_datetime(),
            "price": float(price) if price else None,
            "cash": cash,
            "portfolio": portfolio,
            "position_qty": position.quantity if position else 0,
        })

        # Buy on iteration 1
        if self.iteration == 1:
            print(f"[ITER 1] BEFORE BUY: Cash=${cash:,.2f}, Portfolio=${portfolio:,.2f}, Price=${price:.2f}")
            order = self.create_order(self.mes, 1, "buy")
            self.submit_order(order)

        # Sell on iteration 5
        elif self.iteration == 5 and position and position.quantity > 0:
            print(f"[ITER 5] BEFORE SELL: Cash=${cash:,.2f}, Portfolio=${portfolio:,.2f}, Price=${price:.2f}")
            order = self.create_order(self.mes, 1, "sell")
            self.submit_order(order)

    def on_filled_order(self, position, order, price, quantity, multiplier):
        """Track fills"""
        cash_after = self.get_cash()
        portfolio_after = self.get_portfolio_value()
        print(f"[FILL] {order.side} @ ${price:.2f} → Cash=${cash_after:,.2f}, Portfolio=${portfolio_after:,.2f}")


@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="Requires DataBento API key for futures data"
)
def test_ultra_simple_buy_hold_sell():
    """
    The simplest possible test:
    1. Start with $100k
    2. Buy 1 MES contract
    3. Hold for a few iterations
    4. Sell 1 MES contract
    5. Print everything and manually verify it makes sense
    """
    print("\n" + "="*80)
    print("ULTRA SIMPLE FUTURES TEST")
    print("="*80)

    # Single day
    tzinfo = pytz.timezone("America/New_York")
    backtesting_start = tzinfo.localize(datetime.datetime(2024, 1, 3, 9, 30))
    backtesting_end = tzinfo.localize(datetime.datetime(2024, 1, 3, 16, 0))

    data_source = DataBentoDataPolarsBacktesting(
        datetime_start=backtesting_start,
        datetime_end=backtesting_end,
        api_key=DATABENTO_API_KEY,
    )

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = UltraSimpleStrategy(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False
    )

    print("\n" + "="*80)
    print("SNAPSHOT ANALYSIS")
    print("="*80)

    for snap in strat.snapshots:
        print(f"\nIteration {snap['iteration']} @ {snap['datetime']}")
        print(f"  Price: ${snap['price']:.2f}")
        print(f"  Cash: ${snap['cash']:,.2f}")
        print(f"  Portfolio: ${snap['portfolio']:,.2f}")
        print(f"  Position: {snap['position_qty']} contracts")

        # Calculate what we expect
        if snap['iteration'] == 1:
            print(f"  ✓ Starting state (no position yet)")

        elif snap['iteration'] == 2:
            # After buy
            print(f"  → After BUY:")
            print(f"    - Cash should drop by margin (~$1,300) + fee ($0.50)")
            print(f"    - Portfolio should equal Cash (not cash + notional value)")

        elif snap['position_qty'] > 0:
            # Holding position
            print(f"  → HOLDING position:")
            print(f"    - Portfolio should track price movements")
            print(f"    - Portfolio should equal Cash (mark-to-market)")

        elif snap['iteration'] > 5 and snap['position_qty'] == 0:
            # After sell
            print(f"  → After SELL:")
            print(f"    - Margin should be released")
            print(f"    - Cash should reflect total P&L minus fees")

    print("\n" + "="*80)
    print("MANUAL CHECKS TO DO:")
    print("="*80)
    print("1. Does cash drop by ~$1,300 after buying? (margin)")
    print("2. Does portfolio equal cash while holding? (not cash + $23,000 notional)")
    print("3. Does portfolio move with price changes?")
    print("4. Does final cash = starting cash + price_change*5 - $1.00 in fees?")
    print("="*80)

    # Calculate final P&L
    if len(strat.snapshots) >= 6:
        entry_snap = strat.snapshots[1]  # After buy
        exit_snap = strat.snapshots[5]   # After sell

        # Get prices from the snapshots when we had position
        entry_price = entry_snap['price']
        exit_price = exit_snap['price']

        price_change = exit_price - entry_price
        expected_pnl = price_change * 5  # MES multiplier
        expected_final_cash = 100000 + expected_pnl - 1.00  # Starting + P&L - fees

        actual_final_cash = exit_snap['cash']

        print(f"\nFINAL P&L CALCULATION:")
        print(f"  Entry price: ${entry_price:.2f}")
        print(f"  Exit price: ${exit_price:.2f}")
        print(f"  Price change: ${price_change:.2f}")
        print(f"  Expected P&L: ${expected_pnl:.2f} (price_change * 5)")
        print(f"  Total fees: $1.00 (2 * $0.50)")
        print(f"  Expected final cash: ${expected_final_cash:,.2f}")
        print(f"  Actual final cash: ${actual_final_cash:,.2f}")
        print(f"  Difference: ${abs(expected_final_cash - actual_final_cash):.2f}")
        print()

        # Simple pass/fail
        if abs(expected_final_cash - actual_final_cash) < 100:
            print("✓ PASS: Final cash matches expected P&L")
        else:
            print("✗ FAIL: Final cash does NOT match expected P&L")


if __name__ == "__main__":
    test_ultra_simple_buy_hold_sell()
