"""Partial executions stay eligible until their true remaining quantity closes."""
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Order, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader


@pytest.mark.parametrize('mode', ['ordinary', 'canceled', 'duplicate_bucket', 'overreported', 'oco', 'price_change'])
def test_partial_execution_continues_without_overfill_or_duplicate_fees(tmp_path, monkeypatch, mode):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('LUMIBOT_TELEMETRY', 'false')
    import socket

    def no_network(*args, **kwargs):
        raise AssertionError('controlled partial execution attempted network IO')

    monkeypatch.setattr(socket.socket, 'connect', no_network)
    asset = Asset('SYNTH')
    index = pd.date_range('2026-03-03T10:00', periods=9, freq='min', tz='America/New_York')
    prices = pd.DataFrame({'open': 100, 'high': 110, 'low': 90, 'close': 100,
                          'volume': 1000}, index=index)
    source = PandasDataBacktesting(index[0].to_pydatetime(), index[-1].to_pydatetime(),
        pandas_data=[Data(asset, prices, timestep='minute')], show_progress_bar=False)
    source.get_backtest_fill_price = lambda order: (
        None if order.is_stop_order() else
        100 + 2 * len(order.transactions) if mode == 'price_change' else
        float(order.limit_price or 100)
    )
    source.get_backtest_fill_quantity = lambda order, broker: Decimal(2 if mode == 'overreported' else 1)
    broker = BacktestingBroker(source)

    class PartialExecution(Strategy):
        def initialize(self):
            self.sleeptime = '1M'
            self.set_market('24/7')
            self.entry = None
            self.exits = None
            self.executions = []

        def on_trading_iteration(self):
            if self.entry is None:
                self.entry = self.create_order(asset, 3, 'buy')
                self.submit_order(self.entry)
                if mode == 'duplicate_bucket':
                    # An overlapping transition snapshot cannot execute one order twice.
                    broker._partially_filled_orders.append(self.entry)

        def on_partially_filled_order(self, position, order, price, quantity, multiplier):
            self.executions.append((order.identifier, self.get_datetime(), float(quantity)))
            if mode == 'canceled':
                self.cancel_order(order)

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.executions.append((order.identifier, self.get_datetime(), float(quantity)))
            if mode == 'oco' and order is self.entry:
                self.exits = self.create_order(asset, 3, 'sell', order_class=Order.OrderClass.OCO,
                    limit_price=105, stop_price=95)
                self.submit_order(self.exits)

    fee = TradingFee(per_contract_fee=2)
    strategy = PartialExecution(broker=broker, budget=100000, benchmark_asset=None,
        risk_free_rate=0, analyze_backtest=False, buy_trading_fees=[fee], sell_trading_fees=[fee])
    trader = Trader(backtest=True)
    trader.add_strategy(strategy)
    try:
        trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, show_indicators=False)
        entry = strategy.entry
        entry_count = sum(Decimal(str(fill.quantity)) for fill in entry.transactions)
        if mode == 'canceled':
            assert entry_count == 1 and entry.is_canceled()
            assert len(strategy.executions) == 1
            assert strategy.cash == 99898
            return
        assert entry_count == 3
        assert entry.is_filled(), (str(entry.status), entry.transactions)
        assert len({(row[0], row[1]) for row in strategy.executions}) == len(strategy.executions)
        assert sum(row[2] for row in strategy.executions if row[0] == entry.identifier) == 3
        if mode == 'oco':
            target = next(child for child in strategy.exits.child_orders if not child.is_stop_order())
            stop = next(child for child in strategy.exits.child_orders if child.is_stop_order())
            assert sum(Decimal(str(fill.quantity)) for fill in target.transactions) == 3
            assert target.is_filled() and stop.is_canceled()
            position = strategy.get_position(asset)
            assert position is None or float(position.quantity) == 0
            assert strategy.cash == 100003  # $15 gross, six execution units * $2.
        else:
            position = strategy.get_position(asset)
            assert position is not None and float(position.quantity) == 3
            if mode == 'price_change':
                assert entry.get_fill_price() == 102
                assert position.avg_fill_price == 102
                assert strategy.cash == 99688  # $306 entry, three execution units * $2.
            else:
                assert strategy.cash == 99694  # $300 entry, three execution units * $2.
    finally:
        broker.cleanup_streams()
