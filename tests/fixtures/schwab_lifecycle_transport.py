"""Bounded synthetic Schwab client transport, exclusively for SDK tests.

Responses are broker inputs, not expected callbacks or strategy outcomes. Actual
submitted specs are retained without correcting quantities, durations or logic.
The SDK serializer, parser, reducer and callback queue remain their native owners.
This fixture does not authenticate, send HTTP, run a Strategy or emulate its loop.
It is not a runtime fallback or evidence of Manager/saved-strategy execution.
"""

from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Event, RLock
from types import SimpleNamespace
from typing import NamedTuple

import httpx

from lumibot.brokers.broker import Broker
from lumibot.brokers.schwab import Schwab
from lumibot.strategies.strategy_executor import StrategyExecutor
from lumibot.trading_builtins import PollingStream


class RequestPause(NamedTuple):
    entered: Event
    release: Event
    timeout: float


class SchwabLifecycleTransport:
    """Script exact-order HTTP replies; no broker calls or order policy.

    Each direct read consumes one queued snapshot patch. None means HTTP 404;
    exhausted reads repeat the latest snapshot, including a latest missing read.
    Successful cancel HTTP replies never manufacture terminal order status.
    Broad history exposes the latest visible snapshots without consuming reads.
    Only explicitly submitted orders may receive a scripted direct-read sequence.
    Recorded status/body describe a prepared reply; delivery distinguishes a
    returned response from a paused RPC or transport exception.
    """

    def __init__(self, *, max_requests=128):
        if type(max_requests) is not int or not 1 <= max_requests <= 10000:
            raise ValueError("A bounded positive request limit is required")
        self.max_requests = max_requests
        self._lock = RLock()
        self._calls = []
        self._attempts = []
        self._base = {}
        self._latest = {}
        self._reads = {}
        self._cancel_replies = {}
        self._submission_replies = deque()
        self._history_replies = deque()
        self._pauses = {}
        self.account_hash = "fixture-account"

    @property
    def calls(self):
        with self._lock:
            return deepcopy(self._calls)

    @property
    def attempted_orders(self):
        with self._lock:
            return deepcopy(self._attempts)

    def _check_capacity(self):
        if len(self._calls) >= self.max_requests:
            raise RuntimeError("Synthetic Schwab request limit exhausted")

    def _check_account(self, account_hash):
        if account_hash != self.account_hash:
            raise ValueError("Request does not target this fixture account")

    def _record(self, method, identifier=None, **fields):
        self._check_capacity()
        call = {
            "sequence": len(self._calls) + 1,
            "method": method,
            "order_id": identifier,
            "delivery": "pending",
            **deepcopy(fields),
        }
        self._calls.append(call)
        return call

    @staticmethod
    def _response(method, identifier, status, body=None, *, location=None):
        url = "https://schwab-fixture.invalid/trader/v1/accounts/fixture-account/orders"
        if identifier is not None:
            url += "/" + str(identifier)
        headers = {"Location": location} if location is not None else {}
        kwargs = {"json": deepcopy(body)} if body is not None else {"content": b""}
        return httpx.Response(status, headers=headers, request=httpx.Request(method, url), **kwargs)

    def _take_pause(self, method, identifier):
        for key in ((method, identifier), (method, None)):
            pending = self._pauses.get(key)
            if pending:
                return pending.popleft()
        return None

    @staticmethod
    def _wait(pause):
        if pause is not None:
            pause.entered.set()
            if not pause.release.wait(pause.timeout):
                raise TimeoutError("Synthetic HTTP request pause timed out")

    def _deliver(self, call, pause, method, identifier, status, body=None, *, location=None):
        try:
            self._wait(pause)
            response = self._response(method, identifier, status, body, location=location)
        except Exception as exc:
            with self._lock:
                call.update(delivery="transport_exception", error_type=type(exc).__name__)
            raise
        with self._lock:
            call["delivery"] = "delivered"
        return response

    def pause_next(self, method, identifier=None, *, timeout=2):
        if method not in {"place_order", "get_order", "cancel_order"}:
            raise ValueError("Only implemented exact-order methods can be paused")
        if not 0 < timeout <= 10:
            raise ValueError("Synthetic request pauses require a bounded timeout")
        pause = RequestPause(Event(), Event(), timeout)
        with self._lock:
            if sum(len(pending) for pending in self._pauses.values()) >= self.max_requests:
                raise ValueError("Synthetic pause limit exhausted")
            self._pauses.setdefault((method, str(identifier) if identifier is not None else None), deque()).append(
                pause
            )
        return pause

    def queue_submission_response(self, status, message=""):
        if type(status) is not int or not 200 <= status <= 599:
            raise ValueError("Invalid synthetic HTTP status")
        with self._lock:
            if len(self._submission_replies) >= self.max_requests:
                raise ValueError("Synthetic reply limit exhausted")
            self._submission_replies.append((status, message))

    def queue_cancel_response(self, identifier, status, message=""):
        if type(status) is not int or not 200 <= status <= 599:
            raise ValueError("Invalid synthetic HTTP status")
        with self._lock:
            identifier = str(identifier)
            if identifier not in self._base:
                raise ValueError("Cannot script an unsubmitted order")
            pending = self._cancel_replies.setdefault(identifier, deque())
            if len(pending) >= self.max_requests:
                raise ValueError("Synthetic reply limit exhausted")
            pending.append((status, message))

    def queue_order_reads(self, identifier, *patches):
        with self._lock:
            identifier = str(identifier)
            if identifier not in self._base:
                raise ValueError("Cannot script an unsubmitted order")
            pending = self._reads.setdefault(identifier, deque())
            if len(pending) + len(patches) > self.max_requests:
                raise ValueError("Synthetic reply limit exhausted")
            for patch in patches:
                if patch is not None and (
                    not isinstance(patch, dict) or str(patch.get("orderId", identifier)) != identifier
                ):
                    raise ValueError("Snapshot patch must retain the submitted order identity")
            pending.extend(deepcopy(patches))

    def queue_history_response(self, snapshots):
        if not isinstance(snapshots, list) or len(snapshots) > self.max_requests:
            raise ValueError("History reply must be a bounded list of broker snapshots")
        if any(not isinstance(value, dict) or not value.get("orderId") for value in snapshots):
            raise ValueError("History snapshots require exact order identifiers")
        with self._lock:
            if len(self._history_replies) >= self.max_requests:
                raise ValueError("Synthetic reply limit exhausted")
            self._history_replies.append(deepcopy(snapshots))

    def place_order(self, account_hash, order_spec):
        self._check_account(account_hash)
        spec = deepcopy(order_spec.build() if hasattr(order_spec, "build") else order_spec)
        with self._lock:
            self._check_capacity()
            status, message = self._submission_replies.popleft() if self._submission_replies else (201, "")
            identifier = None if status >= 400 else f"fixture-{len(self._attempts) + 1}"
            body = {"message": message} if message else None
            call = self._record("place_order", identifier, order_spec=spec, status_code=status, response_body=body)
            self._attempts.append({"order_id": identifier, "order_spec": spec, "status_code": status})
            if identifier is not None:
                snapshot = deepcopy(spec)
                snapshot.update(orderId=identifier, status="WORKING", filledQuantity=0)
                for leg in snapshot.get("orderLegCollection", []):
                    leg["orderLegType"] = leg["instrument"]["assetType"]
                self._base[identifier] = snapshot
                self._latest[identifier] = deepcopy(snapshot)
            pause = self._take_pause("place_order", identifier)
        location = (
            f"https://schwab-fixture.invalid/trader/v1/accounts/fixture-account/orders/{identifier}"
            if identifier
            else None
        )
        return self._deliver(call, pause, "POST", None, status, body, location=location)

    def get_order(self, identifier, account_hash):
        self._check_account(account_hash)
        identifier = str(identifier)
        with self._lock:
            self._check_capacity()
            pending = self._reads.get(identifier)
            if pending:
                patch = pending.popleft()
                self._latest[identifier] = None if patch is None else {**deepcopy(self._base[identifier]), **patch}
            body = deepcopy(self._latest.get(identifier))
            status = 404 if body is None else 200
            call = self._record("get_order", identifier, status_code=status, response_body=body)
            pause = self._take_pause("get_order", identifier)
        return self._deliver(call, pause, "GET", identifier, status, body)

    def get_orders_for_account(self, account_hash, **_kwargs):
        self._check_account(account_hash)
        with self._lock:
            self._check_capacity()
            body = (
                self._history_replies.popleft()
                if self._history_replies
                else [deepcopy(value) for value in self._latest.values() if value is not None]
            )
            call = self._record("get_orders_for_account", status_code=200, response_body=body)
        return self._deliver(call, None, "GET", None, 200, body)

    def cancel_order(self, identifier, account_hash):
        self._check_account(account_hash)
        identifier = str(identifier)
        with self._lock:
            self._check_capacity()
            pending = self._cancel_replies.get(identifier)
            status, message = pending.popleft() if pending else (204, "")
            if identifier not in self._base:
                status, message = 404, "Unknown synthetic order"
            body = {"message": message} if message else None
            call = self._record("cancel_order", identifier, status_code=status, response_body=body)
            pause = self._take_pause("cancel_order", identifier)
        return self._deliver(call, pause, "DELETE", identifier, status, body)


class NativeCallbackQueues:
    """A data recipient using SDK enqueue semantics; no Strategy is instantiated."""

    NEW_ORDER = Broker.NEW_ORDER
    PARTIALLY_FILLED_ORDER = Broker.PARTIALLY_FILLED_ORDER
    FILLED_ORDER = Broker.FILLED_ORDER
    CANCELED_ORDER = Broker.CANCELED_ORDER
    ERROR_ORDER = Broker.ERROR_ORDER
    add_event = StrategyExecutor.add_event

    def __init__(self, name):
        self.name = name
        self.queue = Queue()
        self.priority_queue = Queue()
        self._queue_wakeup = Event()


def data_only_schwab_broker(transport, *, name="fixture"):
    """Initialize native Broker storage/queues, without auth, threads or Strategy."""
    broker = Schwab.__new__(Schwab)
    Broker.__init__(
        broker,
        name="synthetic-schwab",
        connect_stream=False,
        start_orders_thread=False,
        data_source=SimpleNamespace(get_datetime=lambda: datetime(2026, 5, 22, tzinfo=timezone.utc)),
    )
    broker.client = transport
    broker.hash_value = transport.account_hash
    broker.schwab_authorization_error = False
    broker._broker_fully_ready = True
    broker._strategy_name = name
    broker._schwab_observed_fill_quantities = {}
    broker._schwab_terminal_observations = set()
    queues = NativeCallbackQueues(name)
    broker._subscribers.append(queues)
    broker.stream = PollingStream(30)
    broker._register_stream_events()
    return broker, queues


def drain_stream_events(broker, *, limit=100):
    """Pump only native queued stream actions, without starting a polling thread."""
    for _ in range(limit):
        try:
            event, payload = broker.stream._queue.get_nowait()
        except Empty:
            return
        try:
            broker.stream._process_queue_event(event, payload)
        finally:
            broker.stream._queue.task_done()
    if not broker.stream._queue.empty():
        raise RuntimeError("Synthetic stream drain limit exhausted")
