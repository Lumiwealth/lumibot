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
from lumibot.backtesting.databento_backtesting_pandas import (
    DataBentoDataBacktestingPandas,
)
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars as DataBentoDataPolarsBacktesting
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG
from lumibot.tools.databento_helper_polars import LUMIBOT_DATABENTO_CACHE_FOLDER

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")

# Contract specs
MES_MULTIPLIER = 5
ES_MULTIPLIER = 50
MES_MARGIN = 1300
ES_MARGIN = 13000


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
        fee_amount = float(fee.flat_fee)
        fee_amount = float(fee.flat_fee)

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

        # If we have both entry and exit, verify P&L and cash bookkeeping exactly
        if len(strat.trades) >= 2:
            entry = strat.trades[0]  # Sell to open
            exit_trade = strat.trades[1]  # Buy to cover

            print("\n" + "-"*80)
            print("P&L VERIFICATION (SHORT TRADE)")
            print("-"*80)

            entry_snapshot = strat.snapshots[0]
            holding_snapshots = [s for s in strat.snapshots if s['position_qty'] < 0]
            assert holding_snapshots, "No holding snapshots recorded for short position"
            sell_snapshot = holding_snapshots[-1]  # still short immediately before closing
            final_snapshot = next((s for s in strat.snapshots if s['position_qty'] == 0 and s['iteration'] > sell_snapshot['iteration']), strat.snapshots[-1])

            margin_deposit = float(entry_snapshot['cash']) - float(entry['cash_after']) - fee_amount
            print(f"  Margin captured: ${margin_deposit:.2f}")
            assert pytest.approx(margin_deposit, abs=0.01) == 1300.0, f"Expected $1,300 margin, got ${margin_deposit:.2f}"

            entry_price = entry['price']
            exit_price = exit_trade['price']
            price_change = entry_price - exit_price  # inverted for short
            expected_pnl = price_change * MES_MULTIPLIER

            print(f"  Entry (sell): ${entry_price:.2f}")
            print(f"  Exit (buy): ${exit_price:.2f}")
            print(f"  Price change (entry - exit): ${price_change:.2f}")
            print(f"  Expected P&L: ${expected_pnl:.2f} (inverted for short)")

            # Mark-to-market validation during the hold window
            for snap in holding_snapshots[:-1]:
                current_price = snap['price']
                if current_price is None:
                    continue
                unrealized = (entry_price - current_price) * MES_MULTIPLIER
                expected_portfolio = float(entry['cash_after']) + margin_deposit + unrealized
                assert pytest.approx(expected_portfolio, abs=0.01) == float(snap['portfolio']), (
                    f"Short unrealized P&L mismatch at {snap['datetime']}: "
                    f"expected ${expected_portfolio:,.2f}, got ${snap['portfolio']:,.2f}"
                )

            assert pytest.approx(float(sell_snapshot['cash']), abs=0.01) == float(entry['cash_after']), \
                "Cash should not drift while holding the short position"

            # Final cash should equal initial cash minus fees plus realized P&L
            starting_cash = float(entry_snapshot['cash'])
            expected_final_cash = starting_cash - 2 * fee_amount + expected_pnl
            print(f"\n  Expected final cash: ${expected_final_cash:,.2f}")
            actual_final_cash = float(final_snapshot['cash'])
            print(f"  Actual final cash:   ${actual_final_cash:,.2f}")
            assert pytest.approx(expected_final_cash, abs=0.01) == actual_final_cash, \
                f"Final cash mismatch: expected ${expected_final_cash:,.2f}, got ${actual_final_cash:,.2f}"

            assert pytest.approx(expected_final_cash, abs=0.01) == float(exit_trade['cash_after']), \
                "Cash reported in exit trade callback should match final ledger"

            print(f"\n✓ PASS: Short selling P&L and cash bookkeeping are exact")

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

        fee_amount = float(fee.flat_fee)
        mes_entry = mes_trades[0]
        mes_exit = mes_trades[1]
        es_entry = es_trades[0]
        es_exit = es_trades[1]

        mes_entry_snapshot = next(s for s in strat.snapshots if s['datetime'] == mes_entry['datetime'])
        es_entry_snapshot = next(s for s in strat.snapshots if s['datetime'] == es_entry['datetime'])

        mes_margin = float(mes_entry_snapshot['cash']) - float(mes_entry['cash_after']) - fee_amount
        es_margin = float(es_entry_snapshot['cash']) - float(es_entry['cash_after']) - fee_amount

        assert pytest.approx(mes_margin, abs=0.01) == MES_MARGIN, \
            f"MES margin should be ${MES_MARGIN}, got ${mes_margin:.2f}"
        assert pytest.approx(es_margin, abs=0.01) == ES_MARGIN, \
            f"ES margin should be ${ES_MARGIN}, got ${es_margin:.2f}"

        print("\n" + "-"*80)
        print("MARK-TO-MARKET VERIFICATION")
        print("-"*80)

        for snap in strat.snapshots:
            expected_portfolio = float(snap['cash'])

            if snap['mes_qty'] != 0 and snap.get('mes_price') is not None:
                expected_portfolio += abs(snap['mes_qty']) * MES_MARGIN
                expected_portfolio += (snap['mes_price'] - mes_entry['price']) * snap['mes_qty'] * MES_MULTIPLIER

            if snap['es_qty'] != 0 and snap.get('es_price') is not None:
                expected_portfolio += abs(snap['es_qty']) * ES_MARGIN
                expected_portfolio += (snap['es_price'] - es_entry['price']) * snap['es_qty'] * ES_MULTIPLIER

            assert pytest.approx(expected_portfolio, abs=0.01) == float(snap['portfolio']), (
                f"Portfolio mismatch at {snap['datetime']}: "
                f"expected ${expected_portfolio:,.2f}, got ${snap['portfolio']:,.2f}"
            )

        mes_pnl = (mes_exit['price'] - mes_entry['price']) * MES_MULTIPLIER
        es_pnl = (es_exit['price'] - es_entry['price']) * ES_MULTIPLIER
        total_pnl = mes_pnl + es_pnl
        total_fees = 4 * fee_amount

        starting_cash = float(strat.snapshots[0]['cash'])
        expected_final_cash = starting_cash - total_fees + total_pnl
        final_cash = float(strat.snapshots[-1]['cash'])

        print(f"\nTotal realised P&L: ${total_pnl:.2f}")
        print(f"Total fees: ${total_fees:.2f}")
        print(f"Expected final cash: ${expected_final_cash:,.2f}")
        print(f"Actual final cash:   ${final_cash:,.2f}")

        assert pytest.approx(expected_final_cash, abs=0.01) == final_cash, \
            f"Final cash mismatch: expected ${expected_final_cash:,.2f}, got ${final_cash:,.2f}"
        assert pytest.approx(expected_final_cash, abs=0.01) == float(es_exit['cash_after']), \
            "Exit trade cash should match ledger final cash"

        print(f"\n✓ PASS: Multiple simultaneous positions tracked with exact accounting")

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
