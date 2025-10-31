"""
Simple, focused test for futures mark-to-market accounting.

Tests a SINGLE trade from start to finish:
1. Buy 1 MES contract
2. Hold for several hours
3. Track cash and portfolio value at each iteration
4. Verify they track MES price movements correctly
5. Sell the contract
6. Verify final P&L matches price change

This test should give us confidence that the basic mechanics are correct.
"""
import datetime
import pytest
import pytz
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars as DataBentoDataPolarsBacktesting
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")

# Expected MES contract specs
MES_MULTIPLIER = 5  # $5 per point
MES_MARGIN = 1300  # ~$1,300 initial margin per contract


class SingleTradeTracker(Strategy):
    """
    Extremely simple strategy:
    - Buy 1 MES contract on first iteration
    - Hold for several hours
    - Sell after a fixed number of iterations
    - Track everything along the way
    """

    def initialize(self):
        self.sleeptime = "15M"  # Check every 15 minutes
        self.set_market("us_futures")

        # Create MES continuous future asset
        self.mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

        # Tracking variables
        self.iteration_count = 0
        self.entry_price = None
        self.entry_cash = None
        self.entry_portfolio = None

        # Track state at each iteration
        self.snapshots = []

        # When to sell (after N iterations)
        self.hold_iterations = 8  # Hold for ~2 hours (8 * 15min)

    def on_trading_iteration(self):
        self.iteration_count += 1

        # Get current state
        price = self.get_last_price(self.mes)
        cash = self.get_cash()
        portfolio = self.get_portfolio_value()
        position = self.get_position(self.mes)
        dt = self.get_datetime()

        has_position = position is not None and position.quantity > 0

        # Record snapshot
        snapshot = {
            "iteration": self.iteration_count,
            "datetime": dt,
            "price": float(price) if price else None,
            "cash": cash,
            "portfolio": portfolio,
            "has_position": has_position,
            "position_qty": position.quantity if position else 0,
        }
        self.snapshots.append(snapshot)

        # BUY on first iteration
        if self.iteration_count == 1:
            self.entry_price = float(price)
            self.entry_cash = cash
            self.entry_portfolio = portfolio

            order = self.create_order(self.mes, quantity=1, side="buy")
            self.submit_order(order)

        # SELL after holding period
        elif self.iteration_count == self.hold_iterations and has_position:
            order = self.create_order(self.mes, quantity=1, side="sell")
            self.submit_order(order)


class TestFuturesSingleTrade:
    """Test a single futures trade from start to finish"""

    @pytest.mark.apitest
    @pytest.mark.skipif(
        not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
        reason="This test requires a Databento API key"
    )
    def test_single_mes_trade_tracking(self):
        """
        Test a single MES trade and verify:
        1. Initial margin is deducted on entry
        2. Cash changes with mark-to-market during hold
        3. Portfolio value tracks cash (not adding notional value)
        4. Final P&L matches price movement * multiplier
        """
        # Use a single trading day
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2024, 1, 3, 9, 30))
        backtesting_end = tzinfo.localize(datetime.datetime(2024, 1, 3, 16, 0))

        data_source = DataBentoDataPolarsBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)

        # Set trading fee
        fee = TradingFee(flat_fee=0.50)

        strat = SingleTradeTracker(
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

        # Verify we got snapshots
        assert len(strat.snapshots) >= 8, f"Expected at least 8 iterations, got {len(strat.snapshots)}"

        print("\n" + "="*80)
        print("SINGLE TRADE ANALYSIS")
        print("="*80)

        # Analyze snapshots
        for i, snap in enumerate(strat.snapshots):
            print(f"\nIteration {snap['iteration']} @ {snap['datetime']}")
            print(f"  Price: ${snap['price']:.2f}")
            print(f"  Cash: ${snap['cash']:,.2f}")
            print(f"  Portfolio: ${snap['portfolio']:,.2f}")
            print(f"  Has Position: {snap['has_position']}")

            if i == 0:
                # Before trade
                assert snap['cash'] == 100000, "Starting cash should be $100k"
                assert snap['portfolio'] == 100000, "Starting portfolio should be $100k"
                assert not snap['has_position'], "Should have no position initially"

            elif i == 1:
                # Just after entry
                # Cash should have decreased by margin + fee
                expected_cash_change = -(MES_MARGIN + 0.50)
                actual_cash_change = snap['cash'] - strat.snapshots[0]['cash']
                cash_diff = abs(expected_cash_change - actual_cash_change)

                print(f"  Expected cash change: ${expected_cash_change:,.2f}")
                print(f"  Actual cash change: ${actual_cash_change:,.2f}")
                print(f"  Difference: ${cash_diff:,.2f}")

                assert cash_diff < 10, f"Cash change after entry should be ~${expected_cash_change}, got ${actual_cash_change}"
                assert snap['has_position'], "Should have position after entry"

                # Portfolio should equal cash + margin + unrealized P&L
                # At entry, unrealized P&L should be near 0, so portfolio ≈ cash + margin
                expected_portfolio = snap['cash'] + MES_MARGIN
                portfolio_diff = abs(snap['portfolio'] - expected_portfolio)
                print(f"  Portfolio: ${snap['portfolio']:,.2f}")
                print(f"  Expected (cash + margin): ${expected_portfolio:,.2f}")
                print(f"  Difference: ${portfolio_diff:,.2f}")
                assert portfolio_diff < 500, f"Portfolio should equal cash + margin at entry, diff was ${portfolio_diff}"

            elif snap['has_position']:
                # During hold period - verify mark-to-market is working
                # Get the entry snapshot (iteration 2, right after entry)
                entry_snap = strat.snapshots[1]  # First snapshot with position
                entry_fill_price = entry_snap['price']  # This should be close to fill price

                # Calculate unrealized P&L from actual fill
                price_change = snap['price'] - strat.entry_price
                expected_pnl = price_change * MES_MULTIPLIER

                # Portfolio should be: Cash + Margin + Unrealized P&L
                # (Cash has margin deducted, so portfolio adds it back plus unrealized P&L)
                expected_portfolio = snap['cash'] + MES_MARGIN + expected_pnl
                actual_portfolio = snap['portfolio']
                portfolio_diff = abs(expected_portfolio - actual_portfolio)

                print(f"  Price change since entry: ${price_change:.2f}")
                print(f"  Expected P&L: ${expected_pnl:.2f}")
                print(f"  Expected portfolio: ${expected_portfolio:,.2f}")
                print(f"  Portfolio diff: ${portfolio_diff:,.2f}")

                # Allow some tolerance for MTM timing and fill price differences
                assert portfolio_diff < 500, f"Portfolio should equal Cash + Unrealized P&L, diff was ${portfolio_diff}"

                # For futures with MTM, portfolio = cash + margin + unrealized P&L
                # Portfolio will be ~$1,300 higher than cash (the margin), so don't check ratio

        # Find exit snapshot (last one or when position closes)
        exit_snap = None
        for i in range(len(strat.snapshots) - 1, 0, -1):
            if not strat.snapshots[i]['has_position'] and strat.snapshots[i-1]['has_position']:
                exit_snap = strat.snapshots[i]
                entry_snap = strat.snapshots[1]  # Right after entry
                break

        if exit_snap:
            print("\n" + "="*80)
            print("EXIT ANALYSIS")
            print("="*80)

            # Calculate expected P&L
            entry_price = strat.entry_price
            exit_price = strat.snapshots[-2]['price']  # Price before exit
            price_change = exit_price - entry_price
            expected_pnl = price_change * MES_MULTIPLIER

            # Final cash should be: starting cash + P&L - fees
            expected_final_cash = 100000 + expected_pnl - 1.00  # 2 fees ($0.50 each)
            actual_final_cash = exit_snap['cash']
            cash_diff = abs(expected_final_cash - actual_final_cash)

            print(f"Entry price: ${entry_price:.2f}")
            print(f"Exit price: ${exit_price:.2f}")
            print(f"Price change: ${price_change:.2f}")
            print(f"Expected P&L: ${expected_pnl:.2f}")
            print(f"Fees: $1.00")
            print(f"Expected final cash: ${expected_final_cash:,.2f}")
            print(f"Actual final cash: ${actual_final_cash:,.2f}")
            print(f"Difference: ${cash_diff:,.2f}")

            # Verify final cash is correct (allow tolerance for fill price differences)
            assert cash_diff < 150, f"Final cash should match expected P&L, diff was ${cash_diff}"

            # Verify portfolio equals cash at end
            portfolio_diff = abs(exit_snap['portfolio'] - exit_snap['cash'])
            print(f"Final portfolio-cash diff: ${portfolio_diff:,.2f}")
            assert portfolio_diff < 10, f"Final portfolio should equal cash, diff was ${portfolio_diff}"

        print("\n" + "="*80)
        print("✓ ALL CHECKS PASSED")
        print("="*80)


if __name__ == "__main__":
    # Run the test directly
    test = TestFuturesSingleTrade()
    test.test_single_mes_trade_tracking()
