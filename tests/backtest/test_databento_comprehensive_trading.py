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
from lumibot.backtesting.databento_backtesting_polars import (
    DataBentoDataBacktestingPolars as DataBentoDataPolarsBacktesting,
)
from lumibot.backtesting.databento_backtesting_pandas import (
    DataBentoDataBacktestingPandas,
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

        snapshots_by_symbol = {}
        for snap in strat.snapshots:
            symbol = snap.get("current_asset")
            if symbol:
                snapshots_by_symbol.setdefault(symbol, []).append(snap)

        fee_amount = float(fee.flat_fee)

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

                symbol_snapshots = snapshots_by_symbol.get(symbol, [])
                entry_snapshot = next((s for s in symbol_snapshots if s.get("phase") == "BUY"), None)
                sell_snapshot = next((s for s in symbol_snapshots if s.get("phase") == "SELL"), None)
                hold_snapshots = [s for s in symbol_snapshots if s.get("phase") == "HOLD"]

                assert entry_snapshot is not None, f"No entry snapshot recorded for {symbol}"
                assert sell_snapshot is not None, f"No sell snapshot recorded for {symbol}"

                cash_before_entry = float(entry_snapshot["cash"])
                entry_cash_after = float(entry["cash_after"])
                margin_deposit = cash_before_entry - entry_cash_after - fee_amount
                expected_margin_total = expected_margin * float(entry["quantity"])

                print(f"\nCASH / MARGIN STATE:")
                print(f"  Cash before entry: ${cash_before_entry:,.2f}")
                print(f"  Cash after entry: ${entry_cash_after:,.2f}")
                print(f"  Margin captured: ${margin_deposit:,.2f} (expected ${expected_margin_total:,.2f})")
                assert pytest.approx(margin_deposit, abs=0.01) == expected_margin_total, (
                    f"{symbol} margin mismatch: expected ${expected_margin_total:,.2f}, "
                    f"got ${margin_deposit:,.2f}"
                )

                # Verify mark-to-market during hold period is exact
                for snap in hold_snapshots:
                    price = snap.get("price")
                    if price is None:
                        continue
                    unrealized = (price - entry["price"]) * float(entry["quantity"]) * expected_multiplier
                    expected_portfolio = entry_cash_after + margin_deposit + unrealized
                    assert pytest.approx(expected_portfolio, abs=0.01) == float(snap["portfolio"]), (
                        f"{symbol} mark-to-market mismatch at {snap['datetime']}: "
                        f"expected ${expected_portfolio:,.2f}, got ${snap['portfolio']:,.2f}"
                    )

                # Snapshot immediately before exit should have identical cash to post-entry state
                assert pytest.approx(float(sell_snapshot["cash"]), abs=0.01) == entry_cash_after, (
                    f"{symbol} cash prior to exit changed unexpectedly: "
                    f"{sell_snapshot['cash']} vs {entry_cash_after}"
                )

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

                cash_before_entry = float(snapshots_by_symbol[symbol][0]["cash"])
                expected_cash_after_exit = (
                    cash_before_entry
                    - fee_amount  # entry fee
                    - fee_amount  # exit fee
                    + expected_pnl
                )
                print(f"\nCASH RECONCILIATION:")
                print(f"  Expected cash after exit: ${expected_cash_after_exit:,.2f}")
                actual_cash_after_exit = float(exit_trade["cash_after"])
                print(f"  Actual cash after exit:   ${actual_cash_after_exit:,.2f}")
                assert pytest.approx(expected_cash_after_exit, abs=0.01) == actual_cash_after_exit, (
                    f"{symbol} cash after exit mismatch: expected ${expected_cash_after_exit:,.2f}, "
                    f"got ${actual_cash_after_exit:,.2f}"
                )

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
        from lumibot.backtesting.databento_backtesting_pandas import (
            DataBentoDataBacktestingPandas as DataBentoDataBacktesting,
        )

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
