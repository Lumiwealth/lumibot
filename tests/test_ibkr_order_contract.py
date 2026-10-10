from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.entities import Asset, Order
from lumibot.tools.ibkr_order_contract import resolve_order_contract

BEFORE = datetime(2026, 9, 17, tzinfo=timezone.utc)
AFTER = datetime(2026, 9, 23, tzinfo=timezone.utc)


def root():
    return Asset('MGC', asset_type='cont_future', multiplier=10)


def held(expiry=date(2026, 10, 28), quantity=1):
    return SimpleNamespace(asset=Asset('MGC', asset_type='future', expiration=expiry, multiplier=10),
                           quantity=quantity)


def resolve(asset=None, side='buy', quantity=1, positions=(), when=AFTER):
    return resolve_order_contract(asset=asset or root(), side=side, quantity=quantity,
                                  positions=positions, when=when)


def test_resolution_preserves_input_identity_and_uses_simulation_time():
    continuous = root()
    continuous.min_tick = .1
    old = resolve(asset=continuous, when=BEFORE)
    new = resolve(asset=continuous)
    assert old.expiration == date(2026, 10, 28)
    assert new.expiration == date(2026, 12, 29)
    assert old.asset_type == 'future'
    assert old.multiplier == 10
    assert old.min_tick == .1
    assert continuous.asset_type == 'cont_future' and continuous.expiration is None


@pytest.mark.parametrize('position_quantity,side', [(1, 'sell'), (-1, 'buy'), (-1, 'buy_to_cover')])
def test_close_retains_held_contract_after_chart_roll(position_quantity, side):
    position = held(quantity=position_quantity)
    assert resolve(side=side, positions=[position]) is position.asset


def test_ambiguous_calendar_spread_close_requires_explicit_contract():
    with pytest.raises(ValueError, match='multiple expiries'):
        resolve(side='sell', positions=[held(), held(date(2026, 12, 29))])
    explicit = held().asset
    assert resolve(asset=explicit, side='sell', positions=[held(), held(date(2026, 12, 29))]) is explicit


def test_cross_expiry_reversal_cannot_silently_open_wrong_contract():
    with pytest.raises(ValueError, match='reversal spans different contracts'):
        resolve(side='sell', quantity=2, positions=[held()])
    # A reversal within one physical expiry remains the existing supported operation.
    assert resolve(side='sell', quantity=2, positions=[held()], when=BEFORE).expiration == date(2026, 10, 28)


@pytest.mark.parametrize('side', ['buy_to_close', 'sell_to_close', 'buy_to_cover'])
def test_close_only_does_not_open_a_new_expiry(side):
    with pytest.raises(ValueError, match='No matching held'):
        resolve(side=side)


def test_protective_children_bind_to_parent_contract_at_submission():
    source = InteractiveBrokersRESTBacktesting.__new__(InteractiveBrokersRESTBacktesting)
    source.get_datetime = lambda: BEFORE
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.data_source = source
    broker.get_tracked_positions = lambda strategy: []
    broker._audit_enabled = lambda: False
    broker.stream = SimpleNamespace(dispatch=lambda *a, **kw: None)
    wrapper = root()
    order = Order(strategy='test', asset=wrapper, quantity=1, side='buy', order_class='bracket',
                  secondary_limit_price=120, secondary_stop_price=90)
    assert len(order.child_orders) == 2
    broker._submit_order(order)
    assert order.asset.expiration == date(2026, 10, 28)
    assert all(child.asset is order.asset for child in order.child_orders)
    assert wrapper.expiration is None
    source.get_datetime = lambda: AFTER
    assert all(child.asset.expiration == date(2026, 10, 28) for child in order.child_orders)


def test_router_only_resolves_ibkr_and_keeps_other_providers_unchanged():
    source = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    source.get_datetime = lambda: AFTER
    order = Order(strategy='test', asset=root(), quantity=1, side='buy')
    source._provider_spec_for_asset = lambda asset: SimpleNamespace(provider='ibkr')
    assert source.resolve_order_asset(order, []).expiration == date(2026, 12, 29)
    source._provider_spec_for_asset = lambda asset: SimpleNamespace(provider='databento')
    assert source.resolve_order_asset(order, []) is order.asset


def test_auto_expiry_wrapper_uses_same_contract_as_continuous_history():
    asset = Asset('MGC', asset_type='future', auto_expiry='front_month', multiplier=10)
    assert resolve(asset=asset).expiration == date(2026, 12, 29)
    assert asset.expiration is None


@pytest.mark.parametrize('position_quantity,side', [(1, 'sell'), (1, 'sell_to_close'), (-1, 'buy'), (-1, 'buy_to_cover')])
def test_known_held_close_does_not_require_current_chart_contract(monkeypatch, position_quantity, side):
    def unavailable(*args, **kwargs):
        raise ValueError('current chart calendar unavailable')
    monkeypatch.setattr('lumibot.tools.futures_roll.determine_contract_year_month', unavailable)
    position = held(quantity=position_quantity)
    assert resolve(side=side, positions=[position]) is position.asset
    with pytest.raises(ValueError, match='current chart calendar unavailable'):
        resolve(side='buy_to_open')
