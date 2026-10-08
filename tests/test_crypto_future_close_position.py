import datetime
import threading
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Order, Position
from lumibot.strategies import Strategy


def _broker():
    start = datetime.datetime(2026, 9, 4)
    data_source = PandasData(
        datetime_start=start,
        datetime_end=start + datetime.timedelta(days=1),
        pandas_data={},
    )
    return BacktestingBroker(data_source=data_source)


@pytest.mark.parametrize(
    ("position_quantity", "expected_side"),
    [
        (Decimal("2"), Order.OrderSide.SELL),
        (Decimal("-2"), Order.OrderSide.BUY),
    ],
)
def test_crypto_future_close_position_returns_reduce_only_order(position_quantity, expected_side):
    broker = _broker()
    asset = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)
    broker._filled_positions.append(Position("crypto-test", asset, position_quantity))

    close_order = broker.close_position("crypto-test", asset, fraction=0.5)

    assert isinstance(close_order, Order)
    assert close_order.quantity == Decimal("1")
    assert close_order.side == expected_side
    assert close_order.reduce_only is True


def test_backtest_crypto_future_open_fill_close_flattens_position():
    broker = _broker()
    asset = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)
    opening_order = Order(
        "crypto-test",
        asset,
        Decimal("2"),
        side=Order.OrderSide.BUY,
    )
    broker._process_filled_order(opening_order, price=100, quantity=Decimal("2"))

    close_order = broker.close_position("crypto-test", asset)
    broker._process_filled_order(close_order, price=110, quantity=Decimal("2"))

    assert close_order.reduce_only is True
    assert broker.get_tracked_position("crypto-test", asset) is None


def test_non_crypto_close_position_behavior_is_unchanged():
    broker = _broker()
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    broker._filled_positions.append(Position("stock-test", asset, Decimal("3")))

    close_order = broker.close_position("stock-test", asset)

    assert isinstance(close_order, Order)
    assert close_order.quantity == Decimal("3")
    assert close_order.side == Order.OrderSide.SELL
    assert getattr(close_order, "reduce_only", False) is False


def test_sell_all_never_submits_null_orders_for_crypto_futures():
    broker = _broker()
    asset = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)
    broker._filled_positions.append(Position("crypto-test", asset, Decimal("2")))
    broker.submit_orders = MagicMock(return_value=[])

    broker.sell_all("crypto-test", cancel_open_orders=False)

    submitted = broker.submit_orders.call_args.args[0]
    assert len(submitted) == 1
    assert isinstance(submitted[0], Order)
    assert submitted[0].reduce_only is True


def test_submit_orders_rejects_a_null_entry():
    broker = _broker()

    with pytest.raises(ValueError, match="null order"):
        broker.submit_orders([None])


def test_strategy_submit_orders_rejects_a_null_entry_before_validation():
    strategy = Strategy.__new__(Strategy)

    with pytest.raises(ValueError, match="null order"):
        strategy.submit_orders([None])


def test_close_position_uses_the_owning_strategys_quote_asset():
    broker = _broker()
    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    usd = Asset("USD", asset_type=Asset.AssetType.FOREX)
    usdt = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    broker.quote_assets = [usd, usdt]
    broker._subscribers.append(SimpleNamespace(name="other-strategy", quote_asset=usd))
    broker._subscribers.append(SimpleNamespace(name="crypto-test", quote_asset=usdt))
    broker._filled_positions.append(Position("crypto-test", asset, Decimal("2")))
    broker.submit_order = MagicMock(side_effect=lambda order: order)

    close_order = broker.close_position("crypto-test", asset)

    assert close_order.quote == usdt


def test_concurrent_new_order_callbacks_do_not_duplicate_the_same_identifier():
    broker = _broker()
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    first = Order("stock-test", asset, 1, side=Order.OrderSide.BUY)
    second = Order("stock-test", asset, 1, side=Order.OrderSide.BUY)
    second.identifier = first.identifier
    original_lookup = broker.get_tracked_order
    second_lookup_started = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def racing_lookup(identifier):
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        if call_number == 1:
            second_lookup_started.wait(timeout=0.2)
        else:
            second_lookup_started.set()
        return original_lookup(identifier)

    broker.get_tracked_order = racing_lookup
    results = []

    def process(order):
        results.append(broker._process_new_order(order))

    threads = [threading.Thread(target=process, args=(order,)) for order in (first, second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1)

    assert not any(thread.is_alive() for thread in threads)
    assert len([order for order in broker._new_orders if order.identifier == first.identifier]) == 1
    assert len(results) == 2
    assert results[0] is results[1]
