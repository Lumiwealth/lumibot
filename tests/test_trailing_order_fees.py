"""Market-triggered trailing orders pay the configured taker execution fees."""
from decimal import Decimal
from types import SimpleNamespace

import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker
from lumibot.entities import Asset, Order, TradingFee


@pytest.mark.parametrize('side, expected', [('buy', Decimal('2.75')), ('sell', Decimal('5.50'))])
@pytest.mark.parametrize('trail', [{'trail_percent': 0.03}, {'trail_price': 3}])
def test_trailing_fill_charges_the_selected_side_fee(side, expected, trail):
    broker = object.__new__(BacktestingBroker)
    strategy = SimpleNamespace(
        buy_trading_fees=[TradingFee(flat_fee=1.25, percent_fee=0.001, per_contract_fee=0.05)],
        sell_trading_fees=[TradingFee(flat_fee=2.50, percent_fee=0.002, per_contract_fee=0.10)],
    )
    order = Order('neutral-fee-check', Asset('SYNTH'), 10, side,
                  order_type=Order.OrderType.TRAIL, **trail)
    assert broker.calculate_trade_cost(order, strategy, 100) == expected


@pytest.mark.parametrize('side, expected', [('buy', Decimal('1.85')), ('sell', Decimal('3.70'))])
def test_trailing_partial_fill_uses_the_executed_quantity(side, expected):
    broker = object.__new__(BacktestingBroker)
    strategy = SimpleNamespace(
        buy_trading_fees=[TradingFee(flat_fee=1.25, percent_fee=0.001, per_contract_fee=0.05)],
        sell_trading_fees=[TradingFee(flat_fee=2.50, percent_fee=0.002, per_contract_fee=0.10)],
    )
    order = Order('neutral-fee-check', Asset('SYNTH'), 10, side, trail_percent=0.03)
    assert broker.calculate_trade_cost(order, strategy, 100, quantity=4) == expected


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_trailing_market_fill_does_not_charge_a_maker_only_fee(side):
    broker = object.__new__(BacktestingBroker)
    fee = TradingFee(percent_fee=0.001, maker=True, taker=False)
    strategy = SimpleNamespace(buy_trading_fees=[fee], sell_trading_fees=[fee])
    order = Order('neutral-fee-check', Asset('SYNTH'), 10, side, trail_percent=0.03)
    assert broker.calculate_trade_cost(order, strategy, 100) == Decimal('0')
