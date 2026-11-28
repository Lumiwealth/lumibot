"""Replays for one Schwab login shared by several bots, and a zero-fill cancel.

Inputs reuse the order and activity shapes already used by
tests/test_schwab_execution_activity.py and tests/fixtures/schwab_lifecycle_transport.py.
No network, no Strategy loop. Each "bot" is its own native Schwab broker object
over one shared synthetic account, the way several live bots on one login run.
"""

import asyncio
import importlib.util
import logging
import time
from datetime import date
from pathlib import Path
from queue import Empty
from threading import Event

import pytest

from lumibot.entities import Asset, Order

_SPEC = importlib.util.spec_from_file_location(
    "shared_login_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py"
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("SCHWAB_CANCEL_DIAGNOSTICS", "false")

    def reject(*_args, **_kwargs):
        raise AssertionError("No external requests in shared-login replay")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def _callbacks(queues):
    events = []
    while True:
        try:
            events.append(queues.priority_queue.get_nowait())
        except Empty:
            return events


def _stock_order(strategy, symbol, quantity=10):
    return Order(strategy=strategy, asset=Asset(symbol), quantity=quantity, side="buy",
                 order_type="limit", limit_price=10, time_in_force="day")


def _two_bots_one_login():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    bot_a, queues_a = _TRANSPORT.data_only_schwab_broker(transport, name="bot-a")
    bot_b, queues_b = _TRANSPORT.data_only_schwab_broker(transport, name="bot-b")
    order_a = _stock_order("bot-a", "AAA")
    order_b = _stock_order("bot-b", "BBB")
    assert bot_a.submit_order(order_a) is order_a
    assert bot_b.submit_order(order_b) is order_b
    _TRANSPORT.drain_stream_events(bot_a)
    _TRANSPORT.drain_stream_events(bot_b)
    _callbacks(queues_a)
    _callbacks(queues_b)
    return transport, (bot_a, queues_a, order_a), (bot_b, queues_b, order_b)


def test_activity_wakeup_rereads_only_each_bots_own_orders_on_a_shared_login():
    transport, (bot_a, queues_a, order_a), (bot_b, queues_b, order_b) = _two_bots_one_login()
    # Both orders fill at the broker. One account-activity message reaches each
    # bot, because both listen on the same login.
    transport.queue_order_reads(order_a.identifier, {"status": "FILLED", "filledQuantity": 10, "averagePrice": 10})
    transport.queue_order_reads(order_b.identifier, {"status": "FILLED", "filledQuantity": 10, "averagePrice": 10})

    before = len(transport.calls)
    bot_a._reconcile_active_schwab_orders()
    reads_a = [call["order_id"] for call in transport.calls[before:] if call["method"] == "get_order"]
    before = len(transport.calls)
    bot_b._reconcile_active_schwab_orders()
    reads_b = [call["order_id"] for call in transport.calls[before:] if call["method"] == "get_order"]

    assert reads_a == [order_a.identifier]
    assert reads_b == [order_b.identifier]
    events_a, events_b = _callbacks(queues_a), _callbacks(queues_b)
    assert [(name, data["order"].identifier) for name, data in events_a] == [(bot_a.FILLED_ORDER, order_a.identifier)]
    assert [(name, data["order"].identifier) for name, data in events_b] == [(bot_b.FILLED_ORDER, order_b.identifier)]


def test_shared_account_history_never_hands_one_bot_the_other_bots_fill():
    transport, (bot_a, queues_a, order_a), (bot_b, queues_b, order_b) = _two_bots_one_login()
    filled_b = transport.get_order(order_b.identifier, transport.account_hash).json()
    filled_b.update(status="FILLED", filledQuantity=10, averagePrice=10)
    transport.queue_history_response([filled_b])

    for raw in bot_a._pull_broker_all_orders():
        observed = bot_a._parse_broker_order(raw, "bot-a")
        if observed is None or bot_a.order_is_foreign_to_local_strategy(observed, "bot-a"):
            continue
        bot_a._process_schwab_order_snapshot(observed, source="rest_history_poll")

    assert bot_a.get_tracked_order(order_b.identifier) is None
    assert order_a.is_active() and order_a.transactions == []
    assert _callbacks(queues_a) == []
    assert len(transport.attempted_orders) == 2


class _SharedLoginServer:
    """One login, one live account stream: a new session closes the older one."""

    def __init__(self):
        self.active = None
        self.logins = {}

    def client_for(self, owner):
        return _SessionClient(self, owner)


class _SessionClient:
    def __init__(self, server, owner):
        self.server = server
        self.owner = owner
        self.kicked = asyncio.Event()
        self.handler = None

    def add_account_activity_handler(self, handler):
        self.handler = handler

    async def login(self):
        previous = self.server.active
        if previous is not None and previous is not self:
            previous.kicked.set()
        self.server.active = self
        self.server.logins[self.owner] = self.server.logins.get(self.owner, 0) + 1

    async def account_activity_sub(self):
        return None

    async def handle_message(self):
        await self.kicked.wait()
        raise ConnectionError("another session opened for this login")


def test_two_bots_on_one_login_back_off_instead_of_fighting_for_the_stream(monkeypatch, caplog):
    """Each bot keeps one warning per disconnect, backs off to the cap, and
    the reconnect count over ten minutes stays bounded."""
    transport = _TRANSPORT.SchwabLifecycleTransport()
    server = _SharedLoginServer()
    bots = {}
    for name in ("bot-a", "bot-b"):
        broker, _queues = _TRANSPORT.data_only_schwab_broker(transport, name=name)
        broker.account_number = "fixture-account"
        broker._schwab_activity_stop = Event()
        broker._schwab_activity_wakeup = Event()
        broker.stream_client = server.client_for(name)
        bots[name] = broker

    clock = [0.0]
    delays = {"bot-a": [], "bot-b": []}
    real_sleep = asyncio.sleep
    horizon_seconds = 600.0

    async def virtual_sleep(delay):
        owner = asyncio.current_task().get_name()
        delays[owner].append(delay)
        clock[0] += delay / 2  # two bots wait in parallel
        if clock[0] >= horizon_seconds:
            for broker in bots.values():
                broker._schwab_activity_stop.set()
        await real_sleep(0)

    def next_client(owner):
        return lambda *_args, **_kwargs: server.client_for(owner)

    monkeypatch.setattr(asyncio, "sleep", virtual_sleep)
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    async def run_both():
        tasks = []
        for name, broker in bots.items():
            async def one(broker=broker, name=name):
                monkeypatch.setattr(
                    "lumibot.brokers.schwab._get_stream_client_class", lambda: next_client(name))
                await broker._run_schwab_account_activity_async()
            tasks.append(asyncio.create_task(one(), name=name))
        while not all(b._schwab_activity_stop.is_set() for b in bots.values()):
            await real_sleep(0)
        for session in [server.active]:
            if session is not None:
                session.kicked.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

    with caplog.at_level(logging.WARNING, logger="lumibot.brokers.schwab"):
        asyncio.run(run_both())

    # Updated 2026-11-28 (test is about a week old): Schwab allows one streamer per
    # login, so after three short sessions in a row a bot stops fighting for the
    # stream and polls order status instead (tests/test_schwab_stream_connection_limit.py).
    for name, sequence in delays.items():
        assert sequence, name
        # Doubling backoff starts as before, never reset by a session that never lasted.
        assert sequence[:2] == [1, 2], (name, sequence)
        # Order-status reconciliation keeps being asked for while the stream is not ours.
        assert bots[name]._schwab_activity_wakeup.is_set()
    messages = [r.getMessage() for r in caplog.records]
    shared = [m for m in messages if "account_activity_stream_shared_login" in m]
    # At least one bot gave up the fight, and each announces it once, not every retry.
    assert 1 <= len(shared) <= 2, shared
    # The old loop reached about 62 logins in ten minutes; polling keeps it small.
    assert sum(server.logins.values()) <= 12, server.logins


def _one_contract_option_order():
    option = Asset("SYN", asset_type="option", expiration=date(2026, 11, 20), strike=10, right="CALL")
    return Order(strategy="fixture", asset=option, quantity=1, side="buy_to_open",
                 order_type="limit", limit_price=1, time_in_force="day")


@pytest.mark.parametrize("path", ["stream_wakeup", "direct", "history_poll"])
def test_zero_fill_cancel_of_one_contract_never_reports_a_partial_fill(path):
    """Exact shape of the customer case: one option contract, cancel accepted,
    Schwab reports CANCELED with an EXECUTION activity of kind CANCELED for the
    whole contract at price 0. Nothing executed, so no fill of any kind."""
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, queues = _TRANSPORT.data_only_schwab_broker(transport)
    order = _one_contract_option_order()
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    _callbacks(queues)

    canceled = {
        "status": "CANCELED", "filledQuantity": 0, "remainingQuantity": 0, "averagePrice": 0,
        "orderActivityCollection": [{
            "activityType": "EXECUTION", "executionType": "CANCELED",
            "executionLegs": [{"legId": 1, "quantity": 1, "price": 0}],
        }],
    }
    transport.queue_order_reads(order.identifier, canceled)
    broker.cancel_order(order)
    if path == "history_poll":
        snapshot = transport.get_order(order.identifier, transport.account_hash).json()
        for _ in range(3):
            transport.queue_history_response([snapshot])
    assert order.status == Order.OrderStatus.CANCELLING

    for _ in range(3):
        if path == "stream_wakeup":
            broker._handle_schwab_account_activity({"content": [{"MESSAGE_TYPE": "UROUT"}]})
            broker._reconcile_active_schwab_orders()
        elif path == "direct":
            broker._refresh_missing_active_order_from_broker(order, order.strategy)
        else:
            for raw in broker._pull_broker_all_orders():
                broker._process_schwab_order_snapshot(
                    broker._parse_broker_order(raw, order.strategy), source="rest_history_poll")

    events = _callbacks(queues)
    names = [name for name, _data in events]
    assert broker.PARTIALLY_FILLED_ORDER not in names
    assert broker.FILLED_ORDER not in names
    assert names.count(broker.CANCELED_ORDER) == 1
    assert order.is_canceled()
    assert order.transactions == []
