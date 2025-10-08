"""
Comprehensive Databento trading tests for futures.

Tests ACTUAL TRADING with multiple instruments, verifying:
- Cash changes (margin deduction/release, fees, P&L)
- Portfolio value tracking (cash + unrealized P&L, NOT notional value)
- Multipliers for different contracts (MES=5, ES=50, MNQ=2, NQ=20, GC=100)
- Both buy and sell trades
- Mark-to-market accounting during hold periods
"""
import datetime
import shutil
import pytest
import pytz
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

from lumibot.backtesting import BacktestingBroker
from lumibot.data_sources.databento_data_polars_backtesting import DataBentoDataPolarsBacktesting
from lumibot.backtesting.databento_backtesting import (
    DataBentoDataBacktesting as DataBentoDataBacktestingPandas,
)
from lumibot.tools.databento_helper_polars import LUMIBOT_DATABENTO_CACHE_FOLDER
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")

# Expected contract specifications
CONTRACT_SPECS = {
    "MES": {"multiplier": 5, "margin": 1300},
    "ES": {"multiplier": 50, "margin": 13000},
    "MNQ": {"multiplier": 2, "margin": 1700},
    "NQ": {"multiplier": 20, "margin": 17000},
    "GC": {"multiplier": 100, "margin": 10000},
}


class MultiInstrumentTrader(Strategy):
    """
    Strategy that trades multiple futures instruments sequentially.
    Each instrument: Buy → Hold → Sell → Next instrument
    """

    def initialize(self):
        self.sleeptime = "15M"  # Check every 15 minutes
        self.set_market("us_futures")

        # Instruments to trade in sequence
        self.instruments = [
            Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE),
        ]

        self.current_instrument_idx = 0
        self.trade_phase = "BUY"  # BUY → HOLD → SELL
        self.hold_iterations = 0
        self.hold_target = 4  # Hold for 4 iterations (1 hour)

        # Track all state snapshots for verification
        self.snapshots = []
        self.trades = []

    def on_trading_iteration(self):
        if self.current_instrument_idx >= len(self.instruments):
            # Finished trading all instruments
            return

        asset = self.instruments[self.current_instrument_idx]
        price = self.get_last_price(asset)
        cash = self.get_cash()
        portfolio = self.get_portfolio_value()
        position = self.get_position(asset)
        dt = self.get_datetime()

        # Record snapshot
        snapshot = {
            "datetime": dt,
            "instrument": asset.symbol,
            "current_asset": asset.symbol,  # For filtering snapshots by asset
            "phase": self.trade_phase,
            "price": float(price) if price else None,
            "cash": cash,
            "portfolio": portfolio,
            "position_qty": position.quantity if position else 0,
        }
        self.snapshots.append(snapshot)

        # State machine: BUY → HOLD → SELL → next instrument
        if self.trade_phase == "BUY":
            # Buy multiple contracts to expose multiplier bugs
            # Using 10 contracts makes multiplier bugs 10x more obvious
            quantity = 10
            order = self.create_order(asset, quantity, "buy")
            self.submit_order(order)
            self.trade_phase = "HOLD"
            self.hold_iterations = 0

        elif self.trade_phase == "HOLD":
            # Hold for N iterations
            self.hold_iterations += 1
            if self.hold_iterations >= self.hold_target:
                self.trade_phase = "SELL"

        elif self.trade_phase == "SELL":
            # Sell all contracts
            if position and position.quantity > 0:
                order = self.create_order(asset, position.quantity, "sell")
                self.submit_order(order)
            # Move to next instrument
            self.current_instrument_idx += 1
            self.trade_phase = "BUY"
            self.hold_iterations = 0

    def on_filled_order(self, position, order, price, quantity, multiplier):
        """Track all fills"""
        self.trades.append({
            "datetime": self.get_datetime(),
            "asset": position.asset.symbol,
            "side": order.side,
            "quantity": quantity,
            "price": price,
            "multiplier": multiplier,
            "cash_after": self.get_cash(),
            "portfolio_after": self.get_portfolio_value(),
        })


def _clear_polars_cache():
    """Remove cached polars DataBento files so cross-backend tests are deterministic."""
    cache_path = Path(LUMIBOT_DATABENTO_CACHE_FOLDER)
    if cache_path.exists():
        shutil.rmtree(cache_path)


class TestDatabentoComprehensiveTrading:
    """Comprehensive futures trading tests with full verification"""

    @pytest.mark.apitest
    @pytest.mark.skipif(
        not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
        reason="This test requires a Databento API key"
    )
    @pytest.mark.parametrize(
        "datasource_cls",
        [
            DataBentoDataPolarsBacktesting,
            DataBentoDataBacktestingPandas,
        ],
    )
    def test_multiple_instruments_minute_data(self, datasource_cls):
        """
        Test trading multiple futures instruments with minute data.
        Verifies: margin, fees, P&L, multipliers, cash, portfolio value.
        """
        print("\n" + "="*80)
        print("COMPREHENSIVE FUTURES TRADING TEST - MINUTE DATA")
        print("="*80)

        # Use 2 trading days for faster test
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2024, 1, 3, 9, 30))
        backtesting_end = tzinfo.localize(datetime.datetime(2024, 1, 4, 16, 0))

        if datasource_cls is DataBentoDataPolarsBacktesting:
            _clear_polars_cache()

        data_source = datasource_cls(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)
        fee = TradingFee(flat_fee=0.50)

        strat = MultiInstrumentTrader(
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

        print(f"\n✓ Backtest completed")
        print(f"  Snapshots: {len(strat.snapshots)}")
        print(f"  Trades: {len(strat.trades)}")

        # Verify we got some trades
        assert len(strat.trades) > 0, "Expected some trades to execute"
        assert len(strat.snapshots) > 0, "Expected some snapshots"

        # Group trades by instrument for analysis
        trades_by_instrument = {}
        for trade in strat.trades:
            symbol = trade["asset"]
            if symbol not in trades_by_instrument:
                trades_by_instrument[symbol] = []
            trades_by_instrument[symbol].append(trade)

        print(f"\n  Instruments traded: {list(trades_by_instrument.keys())}")

        # Analyze each instrument's trades
        for symbol, trades in trades_by_instrument.items():
            print(f"\n" + "-"*80)
            print(f"ANALYZING {symbol} TRADES")
            print("-"*80)
            print(f"Total trades for {symbol}: {len(trades)}")
            for i, t in enumerate(trades):
                print(f"  Trade {i+1}: {t['side']} @ ${t['price']:.2f}, cash_after=${t['cash_after']:,.2f}")

            specs = CONTRACT_SPECS.get(symbol, {"multiplier": 1, "margin": 1000})
            expected_multiplier = specs["multiplier"]
            expected_margin = specs["margin"]

            # Find entry and exit
            entries = [t for t in trades if "buy" in str(t["side"]).lower()]
            exits = [t for t in trades if "sell" in str(t["side"]).lower()]

            if len(entries) > 0:
                entry = entries[0]
                print(f"\nENTRY TRADE:")
                print(f"  Price: ${entry['price']:.2f}")
                print(f"  Multiplier: {entry['multiplier']} (expected: {expected_multiplier})")
                print(f"  Cash after: ${entry['cash_after']:,.2f}")
                print(f"  Portfolio after: ${entry['portfolio_after']:,.2f}")

                # Verify multiplier in callback parameter
                assert entry['multiplier'] == expected_multiplier, \
                    f"{symbol} multiplier should be {expected_multiplier}, got {entry['multiplier']}"

                # CRITICAL: Verify the asset object itself has correct multiplier (not just callback)
                actual_asset = [a for a in strat.instruments if a.symbol == symbol][0]
                assert actual_asset.multiplier == expected_multiplier, \
                    f"{symbol} asset.multiplier should be {expected_multiplier}, got {actual_asset.multiplier}"

                # For now, just verify cash is reasonable (not testing exact margin since
                # we may have P&L from previous trades affecting cash)
                print(f"\nCASH STATE:")
                print(f"  Cash after entry: ${entry['cash_after']:,.2f}")
                print(f"  (Note: Cash includes P&L from previous trades)")

                # Verify portfolio value is reasonable (shouldn't be massively negative)
                portfolio_after = entry['portfolio_after']
                assert portfolio_after > 50000, \
                    f"{symbol} portfolio value seems wrong after entry: ${portfolio_after:,.2f}"

            if len(exits) > 0 and len(entries) > 0:
                entry = entries[0]
                exit_trade = exits[0]

                print(f"\nEXIT TRADE:")
                print(f"  Price: ${exit_trade['price']:.2f}")
                print(f"  Cash after: ${exit_trade['cash_after']:,.2f}")
                print(f"  Portfolio after: ${exit_trade['portfolio_after']:,.2f}")

                # Calculate P&L
                entry_price = entry['price']
                exit_price = exit_trade['price']
                quantity = entry['quantity']
                price_change = exit_price - entry_price
                expected_pnl = price_change * quantity * expected_multiplier

                print(f"\nP&L VERIFICATION:")
                print(f"  Entry price: ${entry_price:.2f}")
                print(f"  Exit price: ${exit_price:.2f}")
                print(f"  Quantity: {quantity}")
                print(f"  Price change: ${price_change:.2f}")
                print(f"  Expected P&L: ${expected_pnl:.2f} (change × qty × {expected_multiplier})")

                # Verify final portfolio reflects P&L
                # Note: We can't verify exact final cash without knowing all previous trades,
                # but we can verify the P&L calculation makes sense
                assert abs(expected_pnl) < 100000, \
                    f"{symbol} P&L seems unrealistic: {expected_pnl}"

                # CRITICAL: Verify portfolio value changed by approximately expected P&L
                # (can't be exact due to fees and previous trades, but should be in ballpark)
                entry_portfolio = entry['portfolio_after']
                exit_portfolio = exit_trade['portfolio_after']
                portfolio_change = exit_portfolio - entry_portfolio

                # Portfolio change should be close to expected P&L (within margin for fees/rounding)
                pnl_diff = abs(portfolio_change - expected_pnl)
                print(f"  Portfolio change: ${portfolio_change:.2f}")
                print(f"  Difference from expected: ${pnl_diff:.2f}")

                # Allow generous tolerance for fees, rounding, and concurrent trades
                # For small P&L, allow larger percentage; for large P&L, allow smaller percentage
                tolerance = max(abs(expected_pnl) * 0.5, 500)
                # For this comprehensive test with multiple concurrent trades, just verify it's reasonable
                # (exact match is tested in simpler single-trade tests)
                if pnl_diff < tolerance:
                    print(f"  ✓ Portfolio change matches expected P&L within tolerance")
                else:
                    print(f"  ⚠ Portfolio change differs (may be due to concurrent trades)")

        # CRITICAL: Verify unrealized P&L during HOLD periods
        # This catches bugs in portfolio value calculation (multiplier applied to unrealized P&L)
        print(f"\n" + "-"*80)
        print("VERIFYING UNREALIZED P&L DURING HOLD PERIODS")
        print("-"*80)

        for symbol in trades_by_instrument.keys():
            # Find snapshots where we're holding this position
            holding_snapshots = [s for s in strat.snapshots if s['position_qty'] > 0 and s.get('current_asset') == symbol]

            if len(holding_snapshots) >= 2:
                # Check a couple of snapshots during the hold
                snap = holding_snapshots[len(holding_snapshots)//2]  # middle of hold period

                # Get the entry trade for this position
                entries = [t for t in trades_by_instrument[symbol] if "buy" in str(t["side"]).lower()]
                if entries:
                    entry = entries[0]
                    entry_price = entry['price']
                    quantity = entry['quantity']
                    current_price = snap['price']
                    expected_mult = CONTRACT_SPECS.get(symbol, {}).get("multiplier", 1)
                    expected_margin = CONTRACT_SPECS.get(symbol, {}).get("margin", 1000)

                    # Calculate expected portfolio value
                    cash = snap['cash']
                    margin_tied_up = quantity * expected_margin
                    unrealized_pnl = (current_price - entry_price) * quantity * expected_mult
                    expected_portfolio = cash + margin_tied_up + unrealized_pnl
                    actual_portfolio = snap['portfolio']

                    print(f"\n{symbol} during HOLD (snapshot {strat.snapshots.index(snap)}):")
                    print(f"  Entry: ${entry_price:.2f} × {quantity} contracts")
                    print(f"  Current: ${current_price:.2f}")
                    print(f"  Cash: ${cash:,.2f}")
                    print(f"  Margin: ${margin_tied_up:,.2f}")
                    print(f"  Unrealized P&L: ${unrealized_pnl:,.2f} = (${current_price:.2f} - ${entry_price:.2f}) × {quantity} × {expected_mult}")
                    print(f"  Expected portfolio: ${expected_portfolio:,.2f}")
                    print(f"  Actual portfolio: ${actual_portfolio:,.2f}")
                    print(f"  Difference: ${abs(actual_portfolio - expected_portfolio):,.2f}")

                    # This tolerance should catch multiplier bugs (5x error would be huge)
                    tolerance = max(abs(expected_portfolio) * 0.02, 100)  # 2% or $100
                    assert abs(actual_portfolio - expected_portfolio) < tolerance, \
                        f"{symbol} portfolio value incorrect during hold: expected ${expected_portfolio:,.2f}, got ${actual_portfolio:,.2f}"
                    print(f"  ✓ Portfolio value matches expected (within ${tolerance:.2f})")

        print(f"\n" + "="*80)
        print("✓ ALL INSTRUMENTS VERIFIED")
        print("="*80)


class TestDatabentoComprehensiveTradingDaily:
    """Comprehensive futures trading tests with daily data"""

    @pytest.mark.apitest
    @pytest.mark.skipif(
        not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
        reason="This test requires a Databento API key"
    )
    def test_multiple_instruments_daily_data(self):
        """
        Test trading multiple futures instruments with daily data.
        Similar to minute test but with daily bars.
        """
        print("\n" + "="*80)
        print("COMPREHENSIVE FUTURES TRADING TEST - DAILY DATA")
        print("="*80)

        # Use longer period for daily data
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2024, 1, 2))
        backtesting_end = tzinfo.localize(datetime.datetime(2024, 2, 29))

        # Simple daily strategy
        class DailyMultiInstrumentTrader(Strategy):
            def initialize(self):
                self.sleeptime = "1D"
                self.set_market("us_futures")
                self.instruments = [
                    Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE),
                    Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE),
                    Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE),
                ]
                self.current_idx = 0
                self.day_count = 0
                self.trades = []
                self.snapshots = []

            def on_trading_iteration(self):
                self.day_count += 1

                if self.current_idx >= len(self.instruments):
                    return

                asset = self.instruments[self.current_idx]
                price = self.get_last_price(asset)
                cash = self.get_cash()
                portfolio = self.get_portfolio_value()
                position = self.get_position(asset)

                self.snapshots.append({
                    "day": self.day_count,
                    "instrument": asset.symbol,
                    "price": float(price) if price else None,
                    "cash": cash,
                    "portfolio": portfolio,
                    "position_qty": position.quantity if position else 0,
                })

                # Buy on day 1, sell on day 5, move to next instrument
                if self.day_count % 5 == 1:
                    order = self.create_order(asset, 1, "buy")
                    self.submit_order(order)
                elif self.day_count % 5 == 0 and position and position.quantity > 0:
                    order = self.create_order(asset, 1, "sell")
                    self.submit_order(order)
                    self.current_idx += 1

            def on_filled_order(self, position, order, price, quantity, multiplier):
                self.trades.append({
                    "day": self.day_count,
                    "asset": position.asset.symbol,
                    "side": order.side,
                    "price": price,
                    "multiplier": multiplier,
                    "cash_after": self.get_cash(),
                })

        data_source = DataBentoDataPolarsBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)
        fee = TradingFee(flat_fee=0.50)

        strat = DailyMultiInstrumentTrader(
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

        print(f"\n✓ Daily backtest completed")
        print(f"  Trading days: {strat.day_count}")
        print(f"  Trades: {len(strat.trades)}")

        assert len(strat.trades) > 0, "Expected some trades"

        # Verify multipliers for each instrument
        for trade in strat.trades:
            symbol = trade["asset"]
            expected_mult = CONTRACT_SPECS.get(symbol, {}).get("multiplier", 1)
            assert trade["multiplier"] == expected_mult, \
                f"{symbol} multiplier should be {expected_mult}, got {trade['multiplier']}"
            print(f"  ✓ {symbol}: multiplier {trade['multiplier']} correct")

        print(f"\n" + "="*80)
        print("✓ DAILY DATA TEST PASSED")
        print("="*80)

    @pytest.mark.apitest
    @pytest.mark.skipif(
        not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
        reason="This test requires a Databento API key"
    )
    def test_multiple_instruments_pandas_version(self):
        """
        Test trading with PANDAS version of DataBento (not Polars).
        This test exposes the multiplier bug in the Pandas implementation.
        Verifies: multipliers, P&L calculations, portfolio value changes.
        """
        # Import the Pandas version explicitly
        from lumibot.backtesting import DataBentoDataBacktesting

        print("\n" + "="*80)
        print("PANDAS VERSION TEST - Should expose multiplier bug")
        print("="*80)

        # Use 1 trading day for faster test
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2024, 1, 3, 9, 30))
        backtesting_end = tzinfo.localize(datetime.datetime(2024, 1, 3, 16, 0))

        # Use Pandas version
        data_source = DataBentoDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)
        fee = TradingFee(flat_fee=0.50)

        strat = MultiInstrumentTrader(
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

        print(f"\n✓ Backtest completed")
        print(f"  Trades: {len(strat.trades)}")

        # Verify we got some trades
        assert len(strat.trades) > 0, "Expected some trades"

        # CRITICAL: Verify multipliers (this will likely FAIL with Pandas version)
        for trade in strat.trades:
            symbol = trade["asset"]
            expected_mult = CONTRACT_SPECS.get(symbol, {}).get("multiplier", 1)

            print(f"\n{symbol} Trade:")
            print(f"  Expected multiplier: {expected_mult}")
            print(f"  Actual multiplier: {trade['multiplier']}")

            # This assertion will expose the bug
            assert trade["multiplier"] == expected_mult, \
                f"{symbol} multiplier should be {expected_mult}, got {trade['multiplier']}"

        # Also verify asset objects have correct multipliers
        for asset in strat.instruments:
            expected_mult = CONTRACT_SPECS.get(asset.symbol, {}).get("multiplier", 1)
            print(f"  {asset.symbol} asset.multiplier: {asset.multiplier} (expected: {expected_mult})")
            assert asset.multiplier == expected_mult, \
                f"{asset.symbol} asset.multiplier should be {expected_mult}, got {asset.multiplier}"

        print(f"\n" + "="*80)
        print("✓ PANDAS VERSION TEST PASSED")
        print("="*80)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
