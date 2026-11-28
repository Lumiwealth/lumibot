"""Open-order reads limited to the markets a reader shows.

Phemex's open-orders REST endpoints need a market. A broker built without a
stream (account and chart reads) therefore scans every active market: 322 on
Nov 20, 2026, about 39 seconds of CCXT rate limiting alone per read, before
network time. A reader that shows one market (a chart) says which markets it
needs, and the read asks the exchange about those only. Offline: REST methods
are stubbed.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.brokers.ccxt import Ccxt


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


def _order(symbol):
    return {"id": f"ord-{symbol}", "clientOrderId": None, "symbol": symbol, "type": "limit",
            "side": "buy", "amount": 1.0, "filled": 0.0, "remaining": 1.0, "price": 100.0,
            "status": "open", "timestamp": 1763600000000, "datetime": "2025-11-20T00:53:20.000Z",
            "average": None, "triggerPrice": None, "stopPrice": None, "info": {}}


def _reader(bases=("BTC", "ETH", "SOL")):
    broker = Ccxt({"exchange_id": "phemex", "apiKey": "k", "secret": "s", "sandbox": False},
                  connect_stream=False, start_orders_thread=False)
    broker.api.set_markets([_swap(base) for base in bases])
    broker.data_source._markets_loaded = True
    broker.api.fetch_open_orders = Mock(side_effect=lambda symbol, *a, **k: [_order(symbol)])
    return broker


def test_scoped_read_asks_only_the_named_market():
    broker = _reader()
    broker.set_order_read_markets(["BTC/USDT:USDT"])

    orders = broker._pull_all_orders("reader", SimpleNamespace(name="reader"))

    assert [call.args[0] for call in broker.api.fetch_open_orders.call_args_list] == ["BTC/USDT:USDT"]
    assert [order.identifier for order in orders] == ["ord-BTC/USDT:USDT"]


def test_unscoped_read_still_scans_every_active_market():
    broker = _reader()

    broker._pull_all_orders("reader", SimpleNamespace(name="reader"))

    assert broker.api.fetch_open_orders.call_count == 3


def test_scope_cleared_with_none():
    broker = _reader()
    broker.set_order_read_markets(["BTC/USDT:USDT"])
    broker.set_order_read_markets(None)

    broker._pull_all_orders("reader", SimpleNamespace(name="reader"))

    assert broker.api.fetch_open_orders.call_count == 3


def test_scope_with_an_unknown_market_is_refused():
    broker = _reader()
    with pytest.raises(ValueError):
        broker.set_order_read_markets(["DOGE/USDT:USDT"])


def _spot(base, quote):
    market = _swap(base)
    market.update(
        id=f"s{base}{quote}", symbol=f"{base}/{quote}", quote=quote, quoteId=quote, settle=None,
        settleId=None, type="spot", spot=True, swap=False, contract=False, linear=None,
        contractSize=None,
    )
    return market


def test_bare_base_symbol_reads_every_market_of_that_base():
    # A crypto order row names its asset by the base alone ("BTC"). Cancelling it must not
    # scan all ~700 Phemex markets (39995 "Too many requests", Phemex testnet, Nov 28 2026):
    # a bare base reads each active market of that base and nothing else.
    broker = _reader(bases=("BTC", "ETH"))
    broker.api.set_markets(
        list(broker.api.markets.values()) + [_spot("BTC", "USDT"), _spot("BTC", "USDC"), _spot("ETH", "USDT")]
    )
    broker.set_order_read_markets(["BTC"])

    broker._pull_all_orders("reader", SimpleNamespace(name="reader"))

    asked = sorted(call.args[0] for call in broker.api.fetch_open_orders.call_args_list)
    assert asked == ["BTC/USDC", "BTC/USDT", "BTC/USDT:USDT"]


def test_bare_symbol_with_no_market_of_that_base_is_refused():
    broker = _reader()
    with pytest.raises(ValueError):
        broker.set_order_read_markets(["DOGE"])
