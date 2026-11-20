"""Account reads on a CCXT broker built without a stream.

Short-lived readers (scheduled jobs, account and chart reads) build the broker
with ``connect_stream=False``. The broker then had no ``stream`` attribute at
all, so ``_pull_broker_all_orders`` failed with
``'Ccxt' object has no attribute 'stream'`` on every open-orders read, for every
CCXT exchange. The call shape below is the one a reader uses:
``broker._pull_all_orders(strategy.name, strategy)``. Offline: exchange classes
are constructed, never connected; REST methods are stubbed.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import ccxt
import pytest

from lumibot.brokers.ccxt import Ccxt
from lumibot.entities import Asset, Order


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")


def _spot_market(exchange):
    amount, price = {ccxt.TICK_SIZE: (0.0001, 0.01), ccxt.DECIMAL_PLACES: (4, 2),
                     ccxt.SIGNIFICANT_DIGITS: (8, 5)}[exchange.precisionMode]
    return {
        "id": "BTCUSDT", "symbol": "BTC/USDT", "base": "BTC", "quote": "USDT",
        "baseId": "BTC", "quoteId": "USDT", "settle": None, "settleId": None,
        "type": "spot", "spot": True, "swap": False, "future": False,
        "option": False, "contract": False, "linear": False, "inverse": False,
        "active": True, "precision": {"amount": amount, "price": price},
        "limits": {"amount": {"min": None, "max": None}, "price": {"min": None, "max": None},
                   "cost": {"min": None, "max": None}}, "info": {},
    }


def _open_order():
    return {"id": "ord-1", "clientOrderId": None, "symbol": "BTC/USDT", "type": "limit",
            "side": "buy", "amount": 0.5, "filled": 0.0, "remaining": 0.5, "price": 20000.0,
            "status": "open", "timestamp": 1763600000000, "datetime": "2025-11-20T00:53:20.000Z",
            "average": None, "triggerPrice": None, "stopPrice": None, "info": {}}


def _reader(exchange_id):
    broker = Ccxt({"exchange_id": exchange_id, "apiKey": "k", "secret": "s", "sandbox": False},
                  connect_stream=False, start_orders_thread=False)
    broker.api.set_markets([_spot_market(broker.api)])
    broker.data_source._markets_loaded = True
    broker.api.fetch_open_orders = Mock(return_value=[_open_order()])
    return broker


@pytest.mark.parametrize("exchange_id", ["phemex", "okx", "kraken", "coinbase", "bybit"])
def test_stream_disabled_broker_reads_open_orders(exchange_id):
    broker = _reader(exchange_id)
    assert broker.stream is None

    strategy = SimpleNamespace(name="reader")
    orders = broker._pull_all_orders(strategy.name, strategy)

    assert [order.identifier for order in orders] == ["ord-1"]
    assert orders[0].asset.symbol == "BTC" and orders[0].side == "buy"
    assert broker.api.fetch_open_orders.called


@pytest.mark.parametrize("exchange_id", ["phemex", "okx"])
def test_stream_disabled_broker_cancels_and_modifies(exchange_id):
    broker = _reader(exchange_id)
    order = Order("reader", Asset("BTC", asset_type="crypto"), Decimal("0.5"), "buy",
                  limit_price=Decimal("20000"), quote=Asset("USDT", asset_type="crypto"))
    order.identifier = "ord-1"
    order.status = "open"

    broker.api.cancel_order = Mock(return_value={"id": "ord-1", "status": "canceled"})
    broker.cancel_order(order)
    assert broker.api.cancel_order.call_args.args[:2] == ("ord-1", "BTC/USDT")

    order.status = "open"
    broker.api.edit_order = Mock(return_value={**_open_order(), "price": 20100.0})
    assert broker._modify_order(order, limit_price=Decimal("20100")) is order
    assert broker.api.edit_order.call_args.args[:3] == ("ord-1", "BTC/USDT", "limit")
