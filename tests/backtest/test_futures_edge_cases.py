"""
Edge case tests for futures trading:
1. Short selling (sell → buy to cover)
2. Multiple simultaneous positions
3. Rapid entry/exit cycles
"""
import datetime
import shutil
from pathlib import Path

import pytest
import pytz
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.databento_backtesting import (
    DataBentoDataBacktesting as DataBentoDataBacktestingPandas,
)
from lumibot.data_sources.databento_data_polars_backtesting import DataBentoDataPolarsBacktesting
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG
from lumibot.tools.databento_helper_polars import LUMIBOT_DATABENTO_CACHE_FOLDER

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")

# Contract specs
MES_MULTIPLIER = 5
ES_MULTIPLIER = 50


def _clear_polars_cache():
    cache_path = Path(LUMIBOT_DATABENTO_CACHE_FOLDER)
    if cache_path.exists():
        shutil.rmtree(cache_path)


class ShortSellingStrategy(Strategy):
    """
    Test short selling:
    - Sell 1 MES contract (open short position)
    - Hold for several iterations
    - Buy to cover (close short position)
    """

    def initialize(self):
        self.sleeptime = "15M"
        self.set_market("us_futures")
        self.mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

        self.iteration = 0
        self.snapshots = []
        self.trades = []

    def on_trading_iteration(self):
        self.iteration += 1

        price = self.get_last_price(self.mes)
        cash = self.get_cash()
        portfolio = self.get_portfolio_value()
        position = self.get_position(self.mes)

        self.snapshots.append({
            "iteration": self.iteration,
            "datetime": self.get_datetime(),
            "price": float(price) if price else None,
            "cash": cash,
            "portfolio": portfolio,
            "position_qty": position.quantity if position else 0,
        })

        # Sell to open short on iteration 1
        if self.iteration == 1:
            order = self.create_order(self.mes, 1, "sell")
            self.submit_order(order)

        # Buy to cover on iteration 6
        elif self.iteration == 6 and position and position.quantity < 0:
            order = self.create_order(self.mes, 1, "buy")
            self.submit_order(order)

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.trades.append({
            "datetime": self.get_datetime(),
            "side": order.side,
            "quantity": quantity,
            "price": price,
            "multiplier": multiplier,
            "cash_after": self.get_cash(),
            "portfolio_after": self.get_portfolio_value(),
        })


class MultiplePositionsStrategy(Strategy):
    """
    Test holding multiple positions simultaneously:
    - Buy MES
    - Buy ES (while still holding MES)
    - Sell MES
    - Sell ES
    """

    def initialize(self):
        self.sleeptime = "15M"
        self.set_market("us_futures")

        self.mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        self.es = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)

        self.iteration = 0
        self.snapshots = []
        self.trades = []

    def on_trading_iteration(self):
        self.iteration += 1

        mes_price = self.get_last_price(self.mes)
        es_price = self.get_last_price(self.es)
        cash = self.get_cash()
        portfolio = self.get_portfolio_value()
        mes_pos = self.get_position(self.mes)
        es_pos = self.get_position(self.es)

        self.snapshots.append({
            "iteration": self.iteration,
            "datetime": self.get_datetime(),
            "mes_price": float(mes_price) if mes_price else None,
            "es_price": float(es_price) if es_price else None,
            "cash": cash,
            "portfolio": portfolio,
            "mes_qty": mes_pos.quantity if mes_pos else 0,
            "es_qty": es_pos.quantity if es_pos else 0,
        })

        # Buy MES on iteration 1
        if self.iteration == 1:
            order = self.create_order(self.mes, 1, "buy")
            self.submit_order(order)

        # Buy ES on iteration 3 (while holding MES)
        elif self.iteration == 3:
            order = self.create_order(self.es, 1, "buy")
            self.submit_order(order)

        # Sell MES on iteration 5
        elif self.iteration == 5 and mes_pos and mes_pos.quantity > 0:
            order = self.create_order(self.mes, 1, "sell")
            self.submit_order(order)

        # Sell ES on iteration 7
        elif self.iteration == 7 and es_pos and es_pos.quantity > 0:
            order = self.create_order(self.es, 1, "sell")
            self.submit_order(order)

    def on_filled_order(self, position, order, price, quantity, multiplier):
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


class TestFuturesEdgeCases:
    """Test edge cases in futures trading"""

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
    def test_short_selling(self, datasource_cls):
        """
        Test short selling:
        1. Sell 1 MES contract (open short)
        2. Hold for several iterations
        3. Buy 1 MES contract (cover short)
        4. Verify P&L is inverse of long trade
        """
        print("\n" + "="*80)
        print("EDGE CASE TEST: SHORT SELLING")
        print("="*80)

        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2024, 1, 3, 9, 30))
        backtesting_end = tzinfo.localize(datetime.datetime(2024, 1, 3, 16, 0))

        if datasource_cls is DataBentoDataPolarsBacktesting:
            _clear_polars_cache()

        data_source = datasource_cls(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)
        fee = TradingFee(flat_fee=0.50)

        strat = ShortSellingStrategy(
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

        # Verify we got at least 2 trades (sell to open, buy to cover)
        assert len(strat.trades) >= 2, f"Expected at least 2 trades, got {len(strat.trades)}"

        # Analyze trades
        print("\n" + "-"*80)
        print("TRADE ANALYSIS")
        print("-"*80)

        for i, trade in enumerate(strat.trades):
            print(f"\nTrade {i+1}:")
            print(f"  Side: {trade['side']}")
            print(f"  Price: ${trade['price']:.2f}")
            print(f"  Multiplier: {trade['multiplier']}")
            print(f"  Cash after: ${trade['cash_after']:,.2f}")
            print(f"  Portfolio after: ${trade['portfolio_after']:,.2f}")

        # Verify multipliers
        for trade in strat.trades:
            assert trade['multiplier'] == MES_MULTIPLIER, \
                f"MES multiplier should be {MES_MULTIPLIER}, got {trade['multiplier']}"

        # If we have both entry and exit, verify P&L
        if len(strat.trades) >= 2:
            entry = strat.trades[0]  # Sell to open
            exit_trade = strat.trades[1]  # Buy to cover

            print("\n" + "-"*80)
            print("P&L VERIFICATION (SHORT TRADE)")
            print("-"*80)

            # For short: P&L = (Entry - Exit) × Qty × Multiplier (inverted!)
            entry_price = entry['price']
            exit_price = exit_trade['price']
            price_change = entry_price - exit_price  # Inverted for short
            expected_pnl = price_change * MES_MULTIPLIER

            print(f"  Entry (sell): ${entry_price:.2f}")
            print(f"  Exit (buy): ${exit_price:.2f}")
            print(f"  Price change (entry - exit): ${price_change:.2f}")
            print(f"  Expected P&L: ${expected_pnl:.2f} (inverted for short)")

            # Verify final cash
            starting_cash = 100000
            total_fees = 1.00  # 2 × $0.50
            expected_final_cash = starting_cash + expected_pnl - total_fees

            # Get final snapshot cash
            final_cash = strat.snapshots[-1]['cash']
            cash_diff = abs(expected_final_cash - final_cash)

            print(f"\n  Starting cash: ${starting_cash:,.2f}")
            print(f"  Expected final cash: ${expected_final_cash:,.2f}")
            print(f"  Actual final cash: ${final_cash:,.2f}")
            print(f"  Difference: ${cash_diff:.2f}")

            # Allow tolerance for timing/fill differences
            assert cash_diff < 150, f"Cash difference too large: ${cash_diff:.2f}"
            print(f"\n✓ PASS: Short selling P&L is correct")

        print("\n" + "="*80)
        print("✓ SHORT SELLING TEST PASSED")
        print("="*80)

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
    def test_multiple_simultaneous_positions(self, datasource_cls):
        """
        Test holding multiple positions at once:
        1. Buy MES
        2. Buy ES (while holding MES)
        3. Verify both positions tracked correctly
        4. Sell MES
        5. Sell ES
        6. Verify final cash is correct
        """
        print("\n" + "="*80)
        print("EDGE CASE TEST: MULTIPLE SIMULTANEOUS POSITIONS")
        print("="*80)

        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2024, 1, 3, 9, 30))
        backtesting_end = tzinfo.localize(datetime.datetime(2024, 1, 3, 16, 0))

        if datasource_cls is DataBentoDataPolarsBacktesting:
            _clear_polars_cache()

        data_source = datasource_cls(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)
        fee = TradingFee(flat_fee=0.50)

        strat = MultiplePositionsStrategy(
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

        # Verify we got 4 trades
        assert len(strat.trades) == 4, f"Expected 4 trades, got {len(strat.trades)}"

        # Group trades by asset
        mes_trades = [t for t in strat.trades if t['asset'] == 'MES']
        es_trades = [t for t in strat.trades if t['asset'] == 'ES']

        print("\n" + "-"*80)
        print("TRADE ANALYSIS")
        print("-"*80)

        print("\nMES Trades:")
        for i, trade in enumerate(mes_trades):
            print(f"  {i+1}. {trade['side']} @ ${trade['price']:.2f}, mult={trade['multiplier']}")

        print("\nES Trades:")
        for i, trade in enumerate(es_trades):
            print(f"  {i+1}. {trade['side']} @ ${trade['price']:.2f}, mult={trade['multiplier']}")

        # Verify multipliers
        for trade in mes_trades:
            assert trade['multiplier'] == MES_MULTIPLIER, \
                f"MES multiplier should be {MES_MULTIPLIER}, got {trade['multiplier']}"

        for trade in es_trades:
            assert trade['multiplier'] == ES_MULTIPLIER, \
                f"ES multiplier should be {ES_MULTIPLIER}, got {trade['multiplier']}"

        # Calculate expected P&L for each instrument
        print("\n" + "-"*80)
        print("P&L VERIFICATION")
        print("-"*80)

        # MES P&L
        mes_entry = mes_trades[0]
        mes_exit = mes_trades[1]
        mes_pnl = (mes_exit['price'] - mes_entry['price']) * MES_MULTIPLIER

        print(f"\nMES:")
        print(f"  Entry: ${mes_entry['price']:.2f}")
        print(f"  Exit: ${mes_exit['price']:.2f}")
        print(f"  P&L: ${mes_pnl:.2f}")

        # ES P&L
        es_entry = es_trades[0]
        es_exit = es_trades[1]
        es_pnl = (es_exit['price'] - es_entry['price']) * ES_MULTIPLIER

        print(f"\nES:")
        print(f"  Entry: ${es_entry['price']:.2f}")
        print(f"  Exit: ${es_exit['price']:.2f}")
        print(f"  P&L: ${es_pnl:.2f}")

        # Total P&L
        total_pnl = mes_pnl + es_pnl
        total_fees = 4.00  # 4 trades × $0.50 each (assuming $0.50 per side)

        print(f"\nTotal:")
        print(f"  Combined P&L: ${total_pnl:.2f}")
        print(f"  Total fees: ${total_fees:.2f}")
        print(f"  Net P&L: ${total_pnl - total_fees:.2f}")

        # Verify final cash
        starting_cash = 100000
        expected_final_cash = starting_cash + total_pnl - total_fees
        final_cash = strat.snapshots[-1]['cash']
        cash_diff = abs(expected_final_cash - final_cash)

        print(f"\n  Starting cash: ${starting_cash:,.2f}")
        print(f"  Expected final cash: ${expected_final_cash:,.2f}")
        print(f"  Actual final cash: ${final_cash:,.2f}")
        print(f"  Difference: ${cash_diff:.2f}")

        # Allow tolerance
        assert cash_diff < 200, f"Cash difference too large: ${cash_diff:.2f}"
        print(f"\n✓ PASS: Multiple simultaneous positions tracked correctly")

        # Verify we had both positions at the same time (iteration 3-4)
        snapshot_with_both = None
        for snap in strat.snapshots:
            if snap['mes_qty'] > 0 and snap['es_qty'] > 0:
                snapshot_with_both = snap
                break

        assert snapshot_with_both is not None, "Should have held both positions simultaneously"
        print(f"\n✓ Verified both positions held simultaneously at iteration {snapshot_with_both['iteration']}")

        print("\n" + "="*80)
        print("✓ MULTIPLE POSITIONS TEST PASSED")
        print("="*80)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
