"""Generic package-unit contracts; no broker request leaves this process."""
from datetime import date, datetime, timezone
from decimal import Decimal
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
              for k in range(100,100+10*len(quantities),10)]
    return [Order('synthetic', asset, quantity, 'buy_to_open' if index % 2 == 0 else 'sell_to_open',
                  quote=Asset('USD', asset_type='forex'))
            for index, (asset, quantity) in enumerate(zip(assets, quantities))]


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


@pytest.mark.parametrize('quantities',[(1,1),(3,3),(2,1),(6,3),(2,3),(3,2),(3,3,3,3),(6,3,6,3)])
def test_alpaca_payload_preserves_every_requested_contract_quantity(quantities):
    payload=capture_payload(quantities)
    actual=[float(payload['qty'])*float(leg['ratio_qty']) for leg in payload['legs']]
    assert actual == list(quantities)


def test_alpaca_credit_package_uses_the_documented_signed_limit():
    # Alpaca REST postorder: mleg positive=debit, negative=credit.
    # https://docs.alpaca.markets/us/reference/postorder
    payload=capture_payload((3,3),order_type='credit',price=2.5)
    assert payload['limit_price'] == -2.5


def test_four_leg_ratio_price_keeps_each_quote_and_reduced_weight():
    helper = OptionsHelper(quote_strategy())
    details = []
    value = helper.calculate_multileg_limit_price(legs((6, 3, 6, 3)), 'mid', price_details=details)
    assert value == pytest.approx(10.0)
    assert [item['ratio'] for item in details] == [2, 1, 2, 1]
    assert [item['price'] for item in details] == [5, 2, 2, 2]
    assert sum(item['signed_price'] for item in details) == pytest.approx(value)


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


@pytest.mark.parametrize('quantities', [(1, 1), (3, 3), (2, 1), (6, 3), (2, 3), (3, 2)])
@pytest.mark.parametrize('closing', [False, True])
def test_ibkr_rest_preserves_signed_contracts_and_net_limit(quantities, closing):
    from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST

    orders = legs(quantities)
    if closing:
        orders[0].side, orders[1].side = Order.OrderSide.SELL_TO_CLOSE, Order.OrderSide.BUY_TO_CLOSE
    captured = {}
    def execute(data):
        captured.update(data['orders'][0])
        raise SubmissionCaptured()
    broker = object.__new__(InteractiveBrokersREST)
    broker.data_source = SimpleNamespace(
        get_conid_from_asset=lambda asset, **kwargs: int(asset.strike),
        get_contract_rules=lambda conid: {'rules': {'increment': .01}},
        execute_order=execute,
    )
    with pytest.raises(SubmissionCaptured):
        broker._submit_orders(orders, is_multileg=True,
                              order_type='credit' if closing else 'debit', price=2.5)
    ratios = [int(part.split('/')[1]) for part in captured['conidex'].split(';;;')[1].split(',')]
    direction = 1 if captured['side'] == 'BUY' else -1
    signed = [float(captured['quantity']) * ratio * direction for ratio in ratios]
    expected = [-quantities[0], quantities[1]] if closing else [quantities[0], -quantities[1]]
    assert signed == expected
    assert captured['price'] == pytest.approx(-2.5 if closing else 2.5)


@pytest.mark.parametrize('quantities', [(1, 1), (3, 3), (2, 1), (6, 3), (2, 3), (3, 2)])
@pytest.mark.parametrize('closing', [False, True])
def test_ibkr_socket_preserves_contracts_and_net_limit(quantities, closing):
    from lumibot.brokers.interactive_brokers import IBApp, InteractiveBrokers

    orders = legs(quantities)
    if closing:
        orders[0].side, orders[1].side = Order.OrderSide.SELL_TO_CLOSE, Order.OrderSide.BUY_TO_CLOSE
    broker = object.__new__(InteractiveBrokers)
    broker._orders_queue = SimpleNamespace(put=Mock())
    parent = broker._submit_orders(orders, is_multileg=True,
        order_type='credit' if closing else 'debit', price=2.5)
    app = object.__new__(IBApp)
    app.nextOrderId = lambda: 1
    app.get_contract_details = lambda asset: [SimpleNamespace(contract=SimpleNamespace(conId=int(asset.strike)))]
    contract, payload = app._create_multileg_order(parent)
    direction = 1 if payload.action == 'BUY' else -1
    actual = [float(payload.totalQuantity) * leg.ratio * direction * (1 if leg.action == 'BUY' else -1)
              for leg in contract.comboLegs]
    expected = [-quantities[0], quantities[1]] if closing else [quantities[0], -quantities[1]]
    assert actual == expected
    assert payload.lmtPrice == pytest.approx(-2.5 if closing else 2.5)


@pytest.mark.parametrize('quantities', [(1, 1), (3, 3), (2, 1), (6, 3), (2, 3), (3, 2)])
@pytest.mark.parametrize('order_type', ['credit', 'debit'])
def test_tradier_retains_absolute_leg_quantities_and_native_price_type(quantities, order_type):
    from lumibot.brokers.tradier import Tradier

    captured = {}
    def submit(**kwargs):
        captured.update(kwargs)
        raise SubmissionCaptured()
    broker = object.__new__(Tradier)
    broker.tradier = SimpleNamespace(orders=SimpleNamespace(multileg_order=submit))
    with pytest.raises(SubmissionCaptured):
        broker._submit_multileg_order(legs(quantities), order_type=order_type, price=2.5)
    assert [leg.quantity for leg in captured['legs']] == list(quantities)
    assert captured['order_type'] == order_type
    assert captured['price'] == 2.5


def test_package_unit_normalization_preserves_decimal_quantities_without_truncation():
    from lumibot.tools.helpers import get_multileg_quantity_units

    count, ratios = get_multileg_quantity_units([Decimal('1.5'), Decimal('.5')])
    assert count == Decimal('.5')
    assert ratios == [3, 1]
    assert [count * ratio for ratio in ratios] == [Decimal('1.5'), Decimal('.5')]


def test_live_initial_package_pricing_weights_the_requested_ratio():
    from lumibot.strategies.strategy import Strategy

    strategy = SimpleNamespace(get_quote=lambda asset, **kwargs: quote_strategy().get_quote(asset))
    best, fastest = Strategy._compute_multileg_net_best_fastest(strategy, legs((6, 3)))
    assert best == pytest.approx(7.7)
    assert fastest == pytest.approx(8.3)


def test_smart_limit_simulation_uses_ratio_weighted_quotes_and_slippage():
    from lumibot.entities import SmartLimitConfig

    children = legs((6, 3))
    for leg in children:
        leg.asset.min_tick = .0001
    parent = Order('synthetic', Asset('XYZ', asset_type='multileg'), 3, 'buy',
        order_class=Order.OrderClass.MULTILEG, child_orders=children,
        smart_limit=SmartLimitConfig(slippage=.1))
    broker = object.__new__(BacktestingBroker)
    broker._audit_enabled = lambda: False
    broker.get_quote = lambda asset, **kwargs: Quote(asset,
        bid=4 if asset.strike == 100 else 1, ask=6 if asset.strike == 100 else 3)
    broker._execute_filled_order = Mock()
    parent._smart_limit_state = {'created_at': None}
    value, timed_out = broker._smart_limit_backtest_price(parent, None, None, None, None)
    assert not timed_out
    assert value == pytest.approx(8.1)
    assert broker._fill_multileg_smart_limit_children(parent, None, value)
    fills = [call.kwargs for call in broker._execute_filled_order.call_args_list]
    assert [fill['filled_quantity'] for fill in fills] == [6, 3]
    assert 2 * fills[0]['price'] - fills[1]['price'] == pytest.approx(8.1, abs=.0003)


@pytest.mark.parametrize('status,filled_packages', [('new', 0), ('partially_filled', 1), ('canceled', 1), ('filled', 3)])
def test_alpaca_ratio_response_preserves_requested_and_executed_contracts(status, filled_packages):
    from alpaca.trading.models import Order as ProviderOrder
    from uuid import UUID

    # Provider schema already exercised against synthetic Dev in polling tests;
    # only neutral identities and the requested/executed ratio quantities vary.
    at = datetime(2030, 6, 3, tzinfo=timezone.utc)
    common = dict(created_at=at, updated_at=at, submitted_at=at, type='limit',
                  time_in_force='day', status=status, extended_hours=False)
    child_responses = [ProviderOrder(**common, id=UUID(int=index), client_order_id=f'leg-{index}',
        asset_id=UUID(int=index+10), symbol=f'XYZ300621C00{strike}000', asset_class='us_option',
        qty=str(3*ratio), filled_qty=str(filled_packages*ratio),
        filled_avg_price=str(price) if filled_packages else None,
        side=side, order_class='simple')
        for index, strike, ratio, price, side in [(1, 100, 2, 5, 'buy'), (2, 110, 1, 2, 'sell')]]
    response = ProviderOrder(**common, id=UUID(int=100), client_order_id='ratio-package',
        qty='3', filled_qty=str(filled_packages), filled_avg_price='8' if filled_packages else None,
        limit_price='8', side='buy', order_class='mleg', legs=child_responses)
    broker = object.__new__(Alpaca)
    parsed = broker._parse_broker_order(response, 'synthetic')
    assert parsed.quantity == 3
    assert parsed.filled_quantity == filled_packages
    assert [child.quantity for child in parsed.child_orders] == [6, 3]
    assert [child.filled_quantity for child in parsed.child_orders] == [2*filled_packages, filled_packages]
    assert [child.parent_identifier for child in parsed.child_orders] == [parsed.identifier]*2
