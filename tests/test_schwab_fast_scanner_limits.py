"""Replay checks for a fast quote-scan / limit-order / cancel loop on Schwab.

Neutral synthetic symbols and recorded-shape Schwab answers only. No broker
credentials, no network, no strategy loop.
"""

import importlib.util
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from lumibot.data_sources.schwab_data import SchwabData, _schwab_market_symbol
from lumibot.entities import Asset, Order

SPEC = importlib.util.spec_from_file_location(
    "schwab_lifecycle_transport",
    Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py",
)
TRANSPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRANSPORT)


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("SCHWAB_CANCEL_DIAGNOSTICS", "false")

    def reject(*_args, **_kwargs):
        raise AssertionError("External network is forbidden in this fixture")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def _call():
    return Asset("LWX", asset_type="option", expiration=date(2026, 12, 18), strike=50, right="CALL")


def _ms(dt):
    return int(dt.timestamp() * 1000)


def _source(payload, status=200, headers=None):
    calls = []

    def get_quotes(symbols):
        calls.append(list(symbols))
        return SimpleNamespace(status_code=status, headers=headers or {}, json=lambda: payload)

    return SchwabData(client=SimpleNamespace(get_quotes=get_quotes), auto_create_client=False), calls


# Item 7: a scanner must be able to tell a stale option quote from a fresh one.
def test_option_quote_keeps_schwab_quote_bid_and_ask_times():
    option = _call()
    symbol = _schwab_market_symbol(option)
    quote_at = datetime(2026, 11, 20, 15, 30, 1, tzinfo=timezone.utc)
    bid_at = datetime(2026, 11, 20, 15, 29, 58, tzinfo=timezone.utc)
    ask_at = datetime(2026, 11, 20, 15, 30, 0, tzinfo=timezone.utc)
    data, calls = _source({
        symbol: {
            "assetMainType": "OPTION",
            "symbol": symbol,
            "quote": {
                "bidPrice": 1.1, "askPrice": 1.2, "lastPrice": 1.15,
                "quoteTime": _ms(quote_at), "bidTime": _ms(bid_at), "askTime": _ms(ask_at),
            },
        }
    })
    quote = data.get_quote(option)
    assert calls == [[symbol]]
    assert quote.quote_time == quote_at
    assert quote.bid_time == bid_at and quote.ask_time == ask_at
    assert quote.timestamp == quote_at


def test_option_quote_without_provider_times_leaves_quote_time_missing():
    option = _call()
    symbol = _schwab_market_symbol(option)
    data, _ = _source({symbol: {"symbol": symbol, "quote": {"bidPrice": 1.1, "askPrice": 1.2}}})
    quote = data.get_quote(option)
    # A strategy checking quote_time for staleness must see "unknown", not a
    # fabricated current time.
    assert quote.quote_time is None and quote.bid_time is None and quote.ask_time is None


# Item 1: a 20-stock basket plus 20 option contracts is one quote request.
def test_twenty_stocks_and_twenty_options_are_one_quote_request():
    stocks = [Asset(f"LW{i:02d}") for i in range(20)]
    options = [
        Asset(f"LW{i:02d}", asset_type="option", expiration=date(2026, 12, 18), strike=50, right="CALL")
        for i in range(20)
    ]
    assets = stocks + options
    payload = {
        _schwab_market_symbol(asset): {"symbol": _schwab_market_symbol(asset), "quote": {"bidPrice": 1, "askPrice": 2}}
        for asset in assets
    }
    data, calls = _source(payload)
    result = data.get_quotes(assets)
    assert len(calls) == 1 and len(calls[0]) == 40
    assert all(result[asset] is not None for asset in assets)


# Item 8: a 429 on quotes is visible to the strategy and is not retried serially.
def test_quote_rate_limit_is_visible_and_not_retried_per_symbol():
    data, calls = _source({}, status=429, headers={"Retry-After": "2"})
    with pytest.raises(ConnectionError, match="429"):
        data.get_quotes([Asset("LW01"), Asset("LW02"), _call()])
    assert len(calls) == 1
    # Inside the Retry-After window the single-quote path also fails visibly
    # (None) without sending another request (changed 2026-11-22: quotes now
    # honor Retry-After; see test_schwab_retry_after_and_reauth.py).
    assert data.get_quote(Asset("LW01")) is None
    assert len(calls) == 1


# Item 6: a short-sale rejection reason must reach the strategy, including
# Schwab error bodies that carry the reason only in the ``errors`` list.
class _ErrorBodyTransport(TRANSPORT.SchwabLifecycleTransport):
    def __init__(self, body):
        super().__init__()
        self._error_body = body

    def place_order(self, account_hash, order_spec):
        self._check_account(account_hash)
        spec = order_spec.build() if hasattr(order_spec, "build") else order_spec
        with self._lock:
            self._record("place_order", None, order_spec=spec, status_code=400, response_body=self._error_body)
            self._attempts.append({"order_id": None, "order_spec": spec, "status_code": 400})
        return httpx.Response(
            400,
            json=self._error_body,
            request=httpx.Request("POST", "https://schwab-fixture.invalid/trader/v1/accounts/fixture-account/orders"),
        )


def _short_sale():
    return Order(
        strategy="fixture",
        asset=Asset("LW"),
        quantity=100,
        side=Order.OrderSide.SELL_SHORT,
        order_type=Order.OrderType.MARKET,
    )


@pytest.mark.parametrize(
    "body",
    [
        {"errors": [{"id": "e1", "status": "400", "title": "Bad Request",
                     "detail": "Synthetic reason: shares not available to borrow"}]},
        {"message": "A validation error occurred while processing the request.",
         "errors": ["Synthetic reason: shares not available to borrow"]},
    ],
)
def test_short_sale_rejection_reason_in_errors_list_reaches_the_strategy(body):
    transport = _ErrorBodyTransport(body)
    broker, queues = TRANSPORT.data_only_schwab_broker(transport)
    order = _short_sale()
    assert broker.submit_order(order) is None
    TRANSPORT.drain_stream_events(broker)
    assert order.status == Order.OrderStatus.ERROR
    assert "not available to borrow" in str(order.error_message)
    events = []
    while not queues.priority_queue.empty():
        events.append(queues.priority_queue.get_nowait())
    assert len(events) == 1 and events[0][0] == broker.ERROR_ORDER
    assert "not available to borrow" in str(events[0][1]["error"])
