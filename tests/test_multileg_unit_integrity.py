"""Generic package-unit contracts; no broker request leaves this process."""
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.backtesting import BacktestingBroker
from lumibot.components.agents.builtins import _backtest_last_trade_multileg_price
from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset, Order, Quote


def legs(quantities):
    assets = [Asset('XYZ',asset_type='option',expiration=date(2030,6,21),strike=k,right='call')
              for k in (100,110)]
    return [Order('synthetic',assets[0],quantities[0],'buy_to_open'),
            Order('synthetic',assets[1],quantities[1],'sell_to_open')]


def quote_strategy(trade_only=False):
    def quote(asset):
        value=5.0 if asset.strike==100 else 2.0
        return Quote(asset,price=value,bid=None if trade_only else value-.1,
                     ask=None if trade_only else value+.1)
    return SimpleNamespace(is_backtesting=True,log_message=Mock(),get_quote=quote,
        get_last_price=lambda asset:5.0 if asset.strike==100 else 2.0,
        get_datetime=lambda:datetime(2030,6,3,tzinfo=timezone.utc))


@pytest.mark.parametrize('quantities,expected', [((1,1),3.0),((3,3),3.0),((2,1),8.0),
                                               ((6,3),8.0),((2,3),4.0),((3,2),11.0)])
def test_quote_package_price_preserves_reduced_leg_ratios(quantities,expected):
    helper=OptionsHelper(quote_strategy())
    assert helper.calculate_multileg_limit_price(legs(quantities),'mid') == pytest.approx(expected)


@pytest.mark.parametrize('quantities,expected', [((1,1),3.0),((3,3),3.0),((2,1),8.0),
                                               ((6,3),8.0),((2,3),4.0),((3,2),11.0)])
def test_trade_bar_package_price_uses_the_same_units(quantities,expected):
    assert _backtest_last_trade_multileg_price(quote_strategy(True),legs(quantities)) == pytest.approx(expected)


class SubmissionCaptured(BaseException):
    """Stop at the real external-write boundary without fabricating a response."""


def capture_payload(quantities,order_type='debit',price=8.0):
    captured={}
    with patch('lumibot.brokers.alpaca.TradingClient'):
        broker=Alpaca({'API_KEY':'YOUR_TEST_API_KEY','API_SECRET':'YOUR_TEST_API_SECRET','PAPER':True},connect_stream=False)
    def capture(*,order_data):
        captured.update(order_data.to_request_fields())
        raise SubmissionCaptured()
    broker.api.submit_order=capture
    with pytest.raises(SubmissionCaptured):
        broker._submit_multileg_order(legs(quantities),order_type=order_type,price=price)
    return captured


@pytest.mark.parametrize('quantities',[(1,1),(3,3),(2,1),(6,3),(2,3),(3,2)])
def test_alpaca_payload_preserves_every_requested_contract_quantity(quantities):
    payload=capture_payload(quantities)
    actual=[float(payload['qty'])*float(leg['ratio_qty']) for leg in payload['legs']]
    assert actual == list(quantities)


def test_alpaca_credit_package_uses_the_documented_signed_limit():
    # Alpaca REST postorder: mleg positive=debit, negative=credit.
    # https://docs.alpaca.markets/us/reference/postorder
    payload=capture_payload((3,3),order_type='credit',price=2.5)
    assert payload['limit_price'] == -2.5


def test_simulation_does_not_fill_a_ratio_package_above_the_ai_debit_limit():
    children = legs((3, 2))
    parent = Order('synthetic', children[0].asset, 1, 'buy', child_orders=children)
    parent._package_net_limit = 4.0
    broker = object.__new__(BacktestingBroker)
    # One generic 3:2 unit costs 3*3 - 2*1 = 7, above the AI's 4 debit cap.
    # Replace only data and execution boundaries; exercise the real limit owner.
    broker._leg_package_fill_price = lambda leg: 3.0 if leg.asset.strike == 100 else 1.0
    broker._execute_filled_order = Mock()
    assert broker._fill_package_at_net_limit(parent, SimpleNamespace()) is False
    broker._execute_filled_order.assert_not_called()


def test_execution_capture_does_not_hide_a_ratio_limit_violation():
    from scripts.agent_eval_production_fixture import _capture_broker_order

    children = legs((3, 2))
    for child, price in zip(children, (3.0, 1.0)):
        child.status = 'fill'
        child.avg_fill_price = price
    parent = Order('synthetic', children[0].asset, 1, 'buy', child_orders=children)
    parent._package_net_limit = 4.0
    execution = _capture_broker_order(parent)['package_execution']
    assert execution['signed_fill_price'] == pytest.approx(7.0)
    assert execution['signed_fill_price'] > execution['signed_limit_price']
