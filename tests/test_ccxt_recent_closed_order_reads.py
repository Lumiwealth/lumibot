"""A reader that shows one market can also see that market's recent finished orders.

Charts draw buy and sell markers from filled orders. CCXT reports a finished order as
``closed``, and that word alone does not mean filled: Phemex also maps ``Deactivated``
to ``closed`` with nothing executed. Offline: REST methods are stubbed, and rows use
CCXT's unified order shape.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.brokers.ccxt import Ccxt
from lumibot.entities import Asset, Order

FILL_MS = 1795370000000


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")


def _swap(base):
    return {
        "id": f"{base}USDT", "symbol": f"{base}/USDT:USDT", "base": base, "quote": "USDT",
        "baseId": base, "quoteId": "USDT", "settle": "USDT", "settleId": "USDT",
        "type": "swap", "spot": False, "swap": True, "future": False, "option": False,
        "contract": True, "linear": True, "inverse": False, "contractSize": 1,
        "active": True, "precision": {"amount": 0.001, "price": 0.1},
        "limits": {"amount": {"min": None, "max": None}, "price": {"min": None, "max": None},
                   "cost": {"min": None, "max": None}, "leverage": {"min": None, "max": None}},
        "info": {},
    }


def _row(order_id, status, filled, amount=0.002, average=68000.5):
    return {"id": order_id, "clientOrderId": None, "symbol": "BTC/USDT:USDT", "type": "market",
            "side": "buy", "amount": amount, "filled": filled, "remaining": amount - filled,
            "price": None, "status": status, "timestamp": FILL_MS - 500,
            "lastTradeTimestamp": FILL_MS if filled else None,
            "average": average if filled else None, "triggerPrice": None, "stopPrice": None,
            "info": {}}


def _reader(open_rows=(), closed_rows=()):
    broker = Ccxt({"exchange_id": "phemex", "apiKey": "k", "secret": "s", "sandbox": False},
                  connect_stream=False, start_orders_thread=False)
    broker.api.set_markets([_swap("BTC"), _swap("ETH")])
    broker.data_source._markets_loaded = True
    broker.api.fetch_open_orders = Mock(return_value=list(open_rows))
    broker.api.fetch_closed_orders = Mock(return_value=list(closed_rows))
    return broker


def _read(broker):
    return broker._pull_all_orders("reader", SimpleNamespace(name="reader"))


def test_recent_read_returns_a_filled_perpetual_order_with_price_quantity_and_time():
    broker = _reader(closed_rows=[_row("filled-1", "closed", 0.002)])
    broker.set_order_read_markets(["BTC/USDT:USDT"])
    broker.set_closed_order_read_window(7)

    [order] = _read(broker)

    assert order.status == "fill"
    assert float(order.filled_quantity) == 0.002
    assert float(order.avg_fill_price) == 68000.5
    assert order.broker_update_date == datetime.fromtimestamp(FILL_MS / 1000, tz=timezone.utc)
    call = broker.api.fetch_closed_orders.call_args
    assert call.args[0] == "BTC/USDT:USDT"
    assert call.kwargs["limit"] == 100
    assert call.kwargs["since"] > 0


def test_closed_with_nothing_executed_is_not_a_fill():
    broker = _reader(closed_rows=[_row("deactivated-1", "closed", 0.0)])
    broker.set_order_read_markets(["BTC/USDT:USDT"])
    broker.set_closed_order_read_window(7)

    [order] = _read(broker)

    assert order.status == "canceled"
    assert not order.is_filled()


def test_closed_after_a_partial_execution_reports_only_what_executed():
    broker = _reader(closed_rows=[_row("partial-1", "closed", 0.001)])
    broker.set_order_read_markets(["BTC/USDT:USDT"])
    broker.set_closed_order_read_window(7)

    [order] = _read(broker)

    assert order.status == "fill"
    assert float(order.filled_quantity) == 0.001


def test_default_read_stays_open_orders_only():
    broker = _reader(open_rows=[_row("open-1", "open", 0.0)], closed_rows=[_row("filled-1", "closed", 0.002)])
    broker.set_order_read_markets(["BTC/USDT:USDT"])

    assert [order.identifier for order in _read(broker)] == ["open-1"]
    broker.api.fetch_closed_orders.assert_not_called()


def test_window_needs_named_markets():
    broker = _reader(closed_rows=[_row("filled-1", "closed", 0.002)])
    broker.set_closed_order_read_window(7)

    assert _read(broker) == []
    broker.api.fetch_closed_orders.assert_not_called()


def test_an_order_in_both_answers_is_returned_once_as_open():
    broker = _reader(open_rows=[_row("same-1", "open", 0.0)], closed_rows=[_row("same-1", "closed", 0.002)])
    broker.set_order_read_markets(["BTC/USDT:USDT"])
    broker.set_closed_order_read_window(7)

    assert [(order.identifier, order.status) for order in _read(broker)] == [("same-1", "open")]


def test_a_failed_history_read_keeps_the_open_orders():
    broker = _reader(open_rows=[_row("open-1", "open", 0.0)])
    broker.api.fetch_closed_orders.side_effect = RuntimeError("exchange unavailable")
    broker.set_order_read_markets(["BTC/USDT:USDT"])
    broker.set_closed_order_read_window(7)

    assert [order.identifier for order in _read(broker)] == ["open-1"]


def test_closed_with_unknown_executed_amount_is_neither_a_fill_nor_a_cancel():
    # Some exchanges leave ``filled`` empty on a finished order. Unknown is not "nothing
    # executed": claiming a cancel could make a reader think a real fill never happened.
    row = _row("unknown-1", "closed", 0.0)
    row["filled"] = None
    broker = _reader(closed_rows=[row])
    broker.set_order_read_markets(["BTC/USDT:USDT"])
    broker.set_closed_order_read_window(7)

    [order] = _read(broker)

    assert order.status == "closed"
    assert not order.is_filled()


def test_filled_order_with_zero_cost_uses_the_reported_average_price():
    # CCXT reads Phemex spot cost from fields the documented order reply can leave at zero,
    # while avgPriceEp still carries the price. A real fill must not become an error.
    broker = _reader()
    row = _row("spot-fill", "closed", 0.002)
    row.update(cost=0.0, average=60000.0)
    broker.api.create_order = Mock(return_value=row)
    asset = broker.get_asset("BTC/USDT:USDT")
    order = Order("s", asset, 0.002, "buy", order_type="market", quote=Asset("USDT", asset_type="crypto"))

    result = broker._submit_order(order)

    assert result.error_message is None
    assert result.is_filled()
    assert float(result.transactions[0].price) == 60000.0


def test_filled_order_with_no_price_at_all_is_still_reported():
    broker = _reader()
    row = _row("no-price", "closed", 0.002)
    row.update(cost=0.0, average=0.0)
    broker.api.create_order = Mock(return_value=row)
    asset = broker.get_asset("BTC/USDT:USDT")
    order = Order("s", asset, 0.002, "buy", order_type="market", quote=Asset("USDT", asset_type="crypto"))

    result = broker._submit_order(order)

    assert "could not be applied" in (result.error_message or "")
