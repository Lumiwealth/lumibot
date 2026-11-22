"""Replay checks for Schwab request pacing.

Covers: one Retry-After cooldown shared by every Schwab market-data call
(quotes, option chains, price history), a client-side per-minute request
budget in which order traffic keeps reserved headroom, and batching of
multi-symbol price reads.

Neutral synthetic symbols and recorded-shape Schwab answers only. No broker
credentials, no network.
"""

import importlib.util
from collections import deque
from datetime import date
from pathlib import Path
from types import SimpleNamespace

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
    for name in (
        "SCHWAB_REQUESTS_PER_MINUTE",
        "SCHWAB_ORDER_RESERVED_REQUESTS_PER_MINUTE",
        "SCHWAB_MARKET_DATA_MAX_WAIT_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)

    def reject(*_args, **_kwargs):
        raise AssertionError("External network is forbidden in this fixture")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def _call(symbol="LWX", strike=50):
    return Asset(symbol, asset_type="option", expiration=date(2026, 12, 18), strike=strike, right="CALL")


def _resp(status, payload=None, headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {}, json=lambda: payload, text="")


CHAIN = {"status": "SUCCESS", "callExpDateMap": {"2026-12-18:26": {"50.0": [{}]}}}
CANDLES = {"candles": [{"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "datetime": 1790000000000}]}


class _MarketDataClient:
    """Replays recorded-shape responses per endpoint; the last one repeats."""

    Options = SimpleNamespace(
        StrikeRange=SimpleNamespace(NTM="NTM", ALL="ALL"),
        ContractType=SimpleNamespace(ALL="ALL"),
        Strategy=SimpleNamespace(SINGLE="SINGLE"),
    )

    def __init__(self, **responses):
        self.responses = {key: deque(value) for key, value in responses.items()}
        self.calls = []

    def _next(self, name):
        self.calls.append(name)
        queue = self.responses[name]
        return queue[0] if len(queue) == 1 else queue.popleft()

    def get_quotes(self, symbols):
        return self._next("get_quotes")

    def get_option_chain(self, **kwargs):
        return self._next("get_option_chain")

    def get_price_history_every_day(self, symbol, **kwargs):
        return self._next("get_price_history_every_day")


def _source(client):
    data = SchwabData(client=client, auto_create_client=False)
    clock = {"value": 2_000.0}
    data._schwab_now = lambda: clock["value"]
    return data, clock


# --- (a) every market-data call honors the same Retry-After cooldown -------


def test_option_chain_429_starts_a_cooldown_for_all_market_data():
    client = _MarketDataClient(
        get_option_chain=[_resp(429, {}, {"Retry-After": "5"}), _resp(200, CHAIN)],
        get_quotes=[_resp(200, {})],
        get_price_history_every_day=[_resp(200, CANDLES)],
    )
    data, clock = _source(client)

    assert data.get_chains(Asset("LWA")) == {}
    assert client.calls == ["get_option_chain"]

    clock["value"] += 1.0
    assert data.get_chains(Asset("LWA")) == {}
    with pytest.raises(ConnectionError, match="rate limit"):
        data.get_chain_full_info(Asset("LWA"), date(2026, 12, 18))
    with pytest.raises(ConnectionError, match="rate limit"):
        data.get_quotes([Asset("LWA")])
    assert data.get_historical_prices(Asset("LWCHAIN"), 3, "day") is None
    assert client.calls == ["get_option_chain"]

    clock["value"] += 5.0
    assert data.get_chains(Asset("LWA"))["Chains"]["CALL"]["2026-12-18"] == [50]
    assert client.calls == ["get_option_chain", "get_option_chain"]


def test_price_history_429_starts_the_shared_market_data_cooldown():
    client = _MarketDataClient(
        get_price_history_every_day=[_resp(429, {}, {"Retry-After": "4"}), _resp(200, CANDLES)],
        get_quotes=[_resp(200, {})],
        get_option_chain=[_resp(200, CHAIN)],
    )
    data, clock = _source(client)

    assert data.get_historical_prices(Asset("LWHIST"), 3, "day") is None
    assert client.calls == ["get_price_history_every_day"]

    clock["value"] += 1.0
    with pytest.raises(ConnectionError, match="rate limit"):
        data.get_quotes([Asset("LWHIST")])
    assert data.get_chains(Asset("LWHIST")) == {}
    assert data.get_historical_prices(Asset("LWHIST"), 3, "day") is None
    assert client.calls == ["get_price_history_every_day"]

    clock["value"] += 4.0
    assert data.get_historical_prices(Asset("LWHIST"), 3, "day") is not None
    assert client.calls == ["get_price_history_every_day", "get_price_history_every_day"]


# --- (b) client-side per-minute budget, orders first -----------------------


def _pacer(rpm, reserve, max_wait=0.0):
    from lumibot.brokers.schwab_request_pacer import SchwabRequestPacer

    clock = {"value": 0.0}
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock["value"] += seconds

    pacer = SchwabRequestPacer(
        requests_per_minute=rpm,
        order_reserved_per_minute=reserve,
        market_data_max_wait_seconds=max_wait,
        clock=lambda: clock["value"],
        sleep=sleep,
    )
    return pacer, clock, sleeps


def test_market_data_cannot_use_the_order_reserve_and_fails_fast_visibly():
    from lumibot.tools.http_retry_after import RateLimitedError

    pacer, _clock, sleeps = _pacer(rpm=10, reserve=3)
    for _ in range(7):
        pacer.acquire("data")
    with pytest.raises(RateLimitedError, match="budget"):
        pacer.acquire("data")
    # Orders still go out immediately from the reserved headroom.
    for _ in range(3):
        pacer.acquire("order")
    assert sleeps == []


def test_orders_are_never_refused_they_wait_for_the_next_token():
    pacer, _clock, sleeps = _pacer(rpm=60, reserve=5)
    for _ in range(60):
        pacer.acquire("order")
    pacer.acquire("order")  # bucket empty: waits about one second, never raises
    assert len(sleeps) == 1 and 0.9 <= sleeps[0] <= 1.1


def test_market_data_waits_within_its_limit_then_proceeds():
    pacer, clock, sleeps = _pacer(rpm=60, reserve=0, max_wait=2.0)
    for _ in range(60):
        pacer.acquire("data")
    pacer.acquire("data")
    assert sleeps and sum(sleeps) <= 2.0
    assert clock["value"] > 0


def test_budget_is_configurable_from_env_and_zero_disables(monkeypatch):
    from lumibot.brokers.schwab_request_pacer import SchwabRequestPacer

    monkeypatch.setenv("SCHWAB_REQUESTS_PER_MINUTE", "30")
    monkeypatch.setenv("SCHWAB_ORDER_RESERVED_REQUESTS_PER_MINUTE", "10")
    monkeypatch.setenv("SCHWAB_MARKET_DATA_MAX_WAIT_SECONDS", "0")
    pacer = SchwabRequestPacer.from_config()
    assert (pacer.requests_per_minute, pacer.order_reserved_per_minute) == (30, 10)
    assert pacer.market_data_max_wait_seconds == 0

    # Config wins over env, so a platform can give each bot its own share.
    pacer = SchwabRequestPacer.from_config({"SCHWAB_REQUESTS_PER_MINUTE": 12, "SCHWAB_ORDER_RESERVED_REQUESTS_PER_MINUTE": 4})
    assert (pacer.requests_per_minute, pacer.order_reserved_per_minute) == (12, 4)

    monkeypatch.setenv("SCHWAB_REQUESTS_PER_MINUTE", "0")
    disabled = SchwabRequestPacer.from_config()
    for _ in range(1_000):
        disabled.acquire("data")


def test_default_budget_is_conservative():
    from lumibot.brokers.schwab_request_pacer import SchwabRequestPacer

    pacer = SchwabRequestPacer.from_config()
    assert pacer.requests_per_minute == 100
    assert 0 < pacer.order_reserved_per_minute < pacer.requests_per_minute


class _QuotingTransport(TRANSPORT.SchwabLifecycleTransport):
    def __init__(self):
        super().__init__()
        self.quote_calls = 0

    def get_quotes(self, symbols):
        self.quote_calls += 1
        payload = {s: {"symbol": s, "quote": {"bidPrice": 1.0, "askPrice": 1.1}} for s in symbols}
        return _resp(200, payload)


def test_quote_polling_never_starves_order_send_and_cancel_on_a_shared_client():
    from lumibot.brokers.schwab_request_pacer import SchwabRequestPacer, attach_pacer
    from lumibot.tools.http_retry_after import RateLimitedError

    transport = _QuotingTransport()
    attach_pacer(transport, SchwabRequestPacer(requests_per_minute=6, order_reserved_per_minute=3,
                                               market_data_max_wait_seconds=0))
    broker, _queues = TRANSPORT.data_only_schwab_broker(transport)
    data = SchwabData(client=transport, auto_create_client=False)

    for _ in range(3):
        data.get_quotes([Asset("LWP")])
    with pytest.raises(RateLimitedError):
        data.get_quotes([Asset("LWP")])
    assert transport.quote_calls == 3

    order = Order(strategy="fixture", asset=Asset("LWP"), quantity=1, side=Order.OrderSide.BUY,
                  order_type=Order.OrderType.LIMIT, limit_price=1.0)
    assert broker.submit_order(order) is order and order.identifier
    broker.cancel_order(order)
    methods = [c["method"] for c in transport.calls]
    assert methods.count("place_order") == 1 and methods.count("cancel_order") == 1


# --- (c) batching and no per-symbol retry under 429 -----------------------


def _scan_assets():
    stocks = [Asset(f"LW{i:02d}") for i in range(20)]
    options = [_call(f"LW{i:02d}") for i in range(20)]
    return stocks + options


def test_last_prices_for_a_20_stock_20_option_scan_is_one_request():
    assets = _scan_assets()
    payload = {
        _schwab_market_symbol(a): {"symbol": _schwab_market_symbol(a), "quote": {"lastPrice": 2.0, "bidPrice": 1.9, "askPrice": 2.1}}
        for a in assets
    }
    client = _MarketDataClient(get_quotes=[_resp(200, payload)])
    data, _clock = _source(client)

    prices = data.get_last_prices(assets)
    assert client.calls == ["get_quotes"]  # was 40 requests, one per asset
    assert all(prices[a] == 2.0 for a in assets)


def test_last_prices_under_429_fail_the_batch_without_per_symbol_retries():
    assets = _scan_assets()
    client = _MarketDataClient(get_quotes=[_resp(429, {}, {"Retry-After": "3"})])
    data, _clock = _source(client)

    prices = data.get_last_prices(assets)
    assert client.calls == ["get_quotes"]
    assert all(prices[a] is None for a in assets)


def test_option_helper_batch_under_rate_limit_does_not_fall_back_to_one_request_per_strike():
    from lumibot.components.options_helper import OptionsHelper
    from lumibot.tools.http_retry_after import RateLimitedError

    single_calls = []

    def get_quotes(assets, quote=None, exchange=None):
        raise RateLimitedError("Schwab quote request failed (status 429)", retry_after_seconds=3.0)

    source = SimpleNamespace(get_quotes=get_quotes)
    broker = SimpleNamespace(IS_BACKTESTING_BROKER=False, option_source=None, data_source=source)

    from lumibot.strategies.strategy import Strategy

    class _Strategy:
        get_quote = Strategy.get_quote

        def log_message(self, *args, **kwargs):
            return None

    strategy = _Strategy()
    strategy.broker = broker
    helper = OptionsHelper(strategy)
    helper._fetch_option_quote = lambda asset, snapshot: single_calls.append(asset)

    helper._prefetch_option_quotes(Asset("LWO"), date(2026, 12, 18), "call", [45, 50, 55])
    assert helper._quote_batch is not None
    assert all(value is None for value in helper._quote_batch.values())
    assert len(helper._quote_batch) == 3
