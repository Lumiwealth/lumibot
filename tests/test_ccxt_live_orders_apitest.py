"""Opt-in LIVE order proof for CCXT exchanges (Coinbase and Kraken).

These tests place real orders with real keys, so they never run by default.
They run only when ``LUMIBOT_LIVE_CCXT_ORDER_TESTS=1`` AND the exchange's keys are set:

* Coinbase spot:            ``COINBASE_API_KEY_NAME`` + ``COINBASE_PRIVATE_KEY`` (or ``COINBASE_API_KEY``/``COINBASE_API_SECRET``)
* Kraken spot:              ``KRAKEN_API_KEY`` + ``KRAKEN_API_SECRET``
* Kraken perpetual:         ``KRAKENFUTURES_API_KEY`` + ``KRAKENFUTURES_API_SECRET``
* Coinbase International perpetual: ``COINBASEINTERNATIONAL_API_KEY``, ``..._API_SECRET``, ``..._API_PASSPHRASE``

What each test does, through LumiBot's real ``Ccxt`` broker:

1. Places the smallest allowed limit BUY about 30% below the market, so it rests and does not fill.
2. Confirms the exchange accepted it (an order id), and that the request carried no CCXT partner ID.
3. Cancels it and confirms the cancel.

Set ``LUMIBOT_LIVE_CCXT_FILL=1`` to also buy the smallest market amount and sell it straight back
(this spends trading fees and crosses the spread). ``<EXCHANGE>_SANDBOX=true`` uses a testnet where
CCXT has one.

Run: ``LUMIBOT_LIVE_CCXT_ORDER_TESTS=1 pytest tests/test_ccxt_live_orders_apitest.py -m apitest -s``
"""

import os
import time
from decimal import Decimal

import pytest

pytestmark = [pytest.mark.apitest]

CCXT_PARTNER_MARKERS = ("ccxt", "CCXT", "nfqkvdjp")

CASES = [
    # (exchange id, credential variables that must be set, symbol, kind)
    ("coinbase", (("COINBASE_API_KEY_NAME", "COINBASE_API_KEY"), ("COINBASE_PRIVATE_KEY", "COINBASE_API_SECRET")),
     "BTC/USDC", "spot"),
    ("kraken", (("KRAKEN_API_KEY",), ("KRAKEN_API_SECRET",)), "BTC/USD", "spot"),
    ("krakenfutures", (("KRAKENFUTURES_API_KEY",), ("KRAKENFUTURES_API_SECRET",)), "BTC/USD:USD", "perpetual"),
    ("coinbaseinternational", (("COINBASEINTERNATIONAL_API_KEY",), ("COINBASEINTERNATIONAL_API_SECRET",),
                               ("COINBASEINTERNATIONAL_API_PASSPHRASE",)), "BTC/USDC:USDC", "perpetual"),
]


def _enabled(groups):
    if os.environ.get("LUMIBOT_LIVE_CCXT_ORDER_TESTS") != "1":
        return False
    return all(any(os.environ.get(name) for name in group) for group in groups)


def _broker(exchange_id):
    from lumibot.brokers.ccxt import Ccxt
    from lumibot.credentials import generic_ccxt_config
    from lumibot.tools.ccxt_exchange_catalog import validate_ccxt_config

    config = generic_ccxt_config(exchange_id)
    validate_ccxt_config(exchange_id, config)
    broker = Ccxt(config, connect_stream=False, start_orders_thread=False)
    broker._ensure_markets_loaded()
    return broker


def _min_amount(broker, symbol, price):
    market = broker.api.markets[symbol]
    limits = market.get("limits") or {}
    amount = Decimal(str((limits.get("amount") or {}).get("min") or 0))
    min_cost = (limits.get("cost") or {}).get("min")
    size = Decimal(str(market.get("contractSize") or 1))
    if min_cost:
        amount = max(amount, Decimal(str(min_cost)) * Decimal("1.1") / (Decimal(str(price)) * size))
    return Decimal(broker.api.amount_to_precision(symbol, max(amount, Decimal("0.00000001"))))


def _assert_no_ccxt_partner_id(broker):
    # The order body is where Coinbase (client_order_id "ccxt-...") and Coinbase International
    # (prefix "nfqkvdjp") put CCXT's ID. Headers are skipped: a user agent may name the library.
    sent = str(broker.api.last_request_body)
    for marker in CCXT_PARTNER_MARKERS:
        assert marker not in sent, f"request still carries a CCXT partner marker: {marker}"


@pytest.mark.parametrize("exchange_id,groups,symbol,kind", CASES, ids=[c[0] for c in CASES])
def test_live_resting_order_is_accepted_and_cancelled(exchange_id, groups, symbol, kind):
    if not _enabled(groups):
        pytest.skip(f"live {exchange_id} order test not enabled (set LUMIBOT_LIVE_CCXT_ORDER_TESTS=1 and keys)")
    from lumibot.entities import Asset, Order

    broker = _broker(exchange_id)
    market = broker.api.markets[symbol]
    last = Decimal(str(broker.api.fetch_ticker(symbol)["last"]))
    price = Decimal(broker.api.price_to_precision(symbol, last * Decimal("0.7")))
    amount = _min_amount(broker, symbol, price)
    if kind == "spot":
        asset, quote = Asset(market["base"], "crypto"), Asset(market["quote"], "crypto")
    else:
        asset, quote = broker.get_asset(symbol), Asset(market["settle"], "crypto")
    order = Order("live-proof", asset, amount, "buy", order_type="limit", limit_price=price, quote=quote)

    submitted = broker._submit_order(order)
    assert submitted.identifier and submitted.status not in ("error",), submitted.error_message
    _assert_no_ccxt_partner_id(broker)
    try:
        time.sleep(1)
        status = broker.api.fetch_order(submitted.identifier, symbol)["status"]
        assert status in ("open", "new"), status
    finally:
        broker.cancel_order(submitted)
    deadline = time.time() + 20
    while time.time() < deadline:
        if broker.api.fetch_order(submitted.identifier, symbol)["status"] in ("canceled", "cancelled", "expired"):
            break
        time.sleep(1)
    else:
        pytest.fail(f"{exchange_id} order {submitted.identifier} was not cancelled; cancel it by hand")


@pytest.mark.parametrize("exchange_id,groups,symbol,kind", CASES, ids=[c[0] for c in CASES])
def test_live_tiny_fill_round_trip(exchange_id, groups, symbol, kind):
    if not _enabled(groups) or os.environ.get("LUMIBOT_LIVE_CCXT_FILL") != "1":
        pytest.skip("set LUMIBOT_LIVE_CCXT_FILL=1 as well to spend fees on a tiny filled round trip")
    from lumibot.entities import Asset, Order

    broker = _broker(exchange_id)
    market = broker.api.markets[symbol]
    last = Decimal(str(broker.api.fetch_ticker(symbol)["last"]))
    amount = _min_amount(broker, symbol, last)
    if kind == "spot":
        asset, quote = Asset(market["base"], "crypto"), Asset(market["quote"], "crypto")
    else:
        asset, quote = broker.get_asset(symbol), Asset(market["settle"], "crypto")
    buy = broker._submit_order(Order("live-proof", asset, amount, "buy", quote=quote))
    assert buy.identifier, buy.error_message
    _assert_no_ccxt_partner_id(broker)
    time.sleep(2)
    sell = Order("live-proof", asset, amount, "sell", quote=quote)
    if kind != "spot":
        sell.reduce_only = True
    sold = broker._submit_order(sell)
    assert sold.identifier, sold.error_message
