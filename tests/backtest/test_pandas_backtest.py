import logging
from datetime import datetime as DateTime
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Order, TradingFee
from lumibot.example_strategies.lifecycle_logger import LifecycleLogger
from lumibot.strategies import Strategy
from lumibot.traders import Trader

from tests.fixtures import pandas_data_fixture


logger = logging.getLogger(__name__)


class TestPandasBacktest:

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None

    def test_pandas_datasource_with_daily_data_in_backtest(self, pandas_data_fixture):
        strategy_name = "LifecycleLogger"
        strategy_class = LifecycleLogger
        backtesting_start = DateTime(2019, 1, 14)
        backtesting_end = DateTime(2019, 1, 20)

        result = strategy_class.backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            name=strategy_name,
            budget=40000,
            show_progress_bar=False,
            quiet_logs=False,
        )
        logger.info(f"Result: {result}")
        assert result is not None


class BracketFeeStrategy(Strategy):
    """Minimal strategy that issues sequential bracket orders for fee validation."""

    def initialize(self):
        self.sleeptime = "1M"
        self.asset = Asset("BRKT", Asset.AssetType.STOCK)
        self.vars.plan = [
            {
                "order_type": Order.OrderType.MARKET,
                "limit_price": 101.0,
                "stop_price": 98.0,
            },
            {
                "order_type": Order.OrderType.LIMIT,
                "entry_price": 98.5,
                "limit_price": 100.5,
                "stop_price": 96.8,
            },
        ]
        self.vars.plan_index = 0

    def on_trading_iteration(self):
        if self.vars.plan_index >= len(self.vars.plan):
            return
        if self.get_position(self.asset) is not None:
            return
        if any(order.status in ("open", "new") for order in self.get_orders()):
            return

        config = self.vars.plan[self.vars.plan_index]
        kwargs = {
            "order_type": config["order_type"],
            "order_class": Order.OrderClass.BRACKET,
            "secondary_limit_price": config["limit_price"],
            "secondary_stop_price": config["stop_price"],
        }
        if config["order_type"] == Order.OrderType.LIMIT:
            kwargs["limit_price"] = config["entry_price"]

        order = self.create_order(self.asset, 10, Order.OrderSide.BUY, **kwargs)
        self.submit_order(order)
        self.vars.plan_index += 1


def _build_bracket_datasource():
    index = pd.date_range(
        "2025-01-02 09:30",
        periods=6,
        freq="min",
        tz="America/New_York",
    )
    df = pd.DataFrame(
        {
            "open": [100.0, 100.0, 99.6, 99.0, 98.5, 97.0],
            "high": [100.0, 102.0, 100.0, 99.2, 99.0, 101.0],
            "low": [100.0, 99.5, 98.8, 98.4, 96.3, 97.0],
            "close": [100.0, 101.5, 99.2, 98.7, 97.2, 100.5],
            "volume": [1000, 1000, 1000, 1000, 1000, 1000],
        },
        index=index,
    )
    asset = Asset("BRKT", Asset.AssetType.STOCK)
    pandas_data = {asset: Data(asset, df)}
    return asset, PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=index[0],
        datetime_end=index[-1],
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )


def _replay_cash_ledger(fills):
    cash = 100000.0
    for _, row in fills.iterrows():
        qty = float(row["filled_quantity"])
        price = float(row["price"])
        fee = float(row["trade_cost"])
        side = row["side"]
        if side in ("buy", "buy_to_open", "buy_to_cover"):
            cash -= qty * price
            cash -= fee
        elif side in ("sell", "sell_to_close", "sell_short", "sell_to_open"):
            cash += qty * price
            cash -= fee
    return cash


def test_bracket_orders_apply_entry_and_exit_fees():
    asset, data_source = _build_bracket_datasource()
    broker = BacktestingBroker(data_source=data_source)
    trading_fee = TradingFee(percent_fee=Decimal("0.001"), maker=True, taker=True)

    strategy = BracketFeeStrategy(
        broker=broker,
        budget=100000.0,
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
    )

    trader = Trader(backtest=True, logfile="")
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)

    trade_log = broker._trade_event_log_df
    fills = trade_log[trade_log["status"] == "fill"].reset_index(drop=True)

    assert len(fills) == 4
    buy_fees = fills[fills["side"] == "buy"]["trade_cost"]
    sell_fees = fills[fills["side"].str.startswith("sell")]["trade_cost"]

    assert all(buy_fees > 0)
    assert all(sell_fees > 0)

    expected_cash = _replay_cash_ledger(fills)
    assert pytest.approx(strategy.cash, rel=1e-9) == expected_cash
