"""
Debug test to trace avg_fill_price through a trade lifecycle
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


class DebugStrategy(Strategy):
    """Debug strategy to trace avg_fill_price"""

    def initialize(self):
        self.sleeptime = "15M"
        self.set_market("us_futures")
        self.mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        self.iteration = 0
        self.trade_done = False

    def on_trading_iteration(self):
        self.iteration += 1

        position = self.get_position(self.mes)
        price = self.get_last_price(self.mes)
        cash = self.get_cash()
        portfolio = self.get_portfolio_value()

        print(f"\n[ITER {self.iteration}] Price=${price:.2f}, Cash=${cash:,.2f}, Portfolio=${portfolio:,.2f}")

        if position:
            print(f"  Position: qty={position.quantity}, avg_fill_price={position.avg_fill_price}")
        else:
            print(f"  Position: None")

        # Buy on iteration 1
        if self.iteration == 1:
            print(f"  >>> SUBMITTING BUY ORDER")
            order = self.create_order(self.mes, 1, "buy")
            self.submit_order(order)

        # Close on iteration 3
        elif self.iteration == 3 and position and position.quantity > 0:
            print(f"  >>> SUBMITTING SELL ORDER")
            order = self.create_order(self.mes, 1, "sell")
            self.submit_order(order)
            self.trade_done = True

    def on_filled_order(self, position, order, price, quantity, multiplier):
        print(f"  [FILL] {order.side} @ ${price:.2f}")
        print(f"    order.avg_fill_price = {order.avg_fill_price}")
        print(f"    position.avg_fill_price = {position.avg_fill_price}")
        print(f"    position.quantity = {position.quantity}")


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key"
)
def test_debug_avg_fill_price():
    """Debug avg_fill_price tracking"""
    print("\n" + "="*80)
    print("DEBUG: AVG_FILL_PRICE TRACKING")
    print("="*80)

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

    strat = DebugStrategy(
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
    print("DEBUG TEST COMPLETE")
    print("="*80)


if __name__ == "__main__":
    test_debug_avg_fill_price()
