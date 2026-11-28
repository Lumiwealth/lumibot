"""Schwab allows one streamer connection per login. A second program on the same
login must not loop on reconnects: it polls order status at a bounded rate, logs
one plain warning, keeps trading, and retries the stream rarely.

Response shapes follow schwab-py's raise path (``exc.response`` holds the raw
streamer message) and the Schwab Streamer Guide error table, where code 12 is
CLOSE_CONNECTION ("maximum number of connections"). No network is used.
"""

import asyncio
import importlib.util
import logging
import time
from pathlib import Path
from threading import Event

import pytest
from schwab.streaming import UnexpectedResponse, UnexpectedResponseCode

from lumibot.brokers.schwab import Schwab

_SPEC = importlib.util.spec_from_file_location(
    "stream_limit_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py"
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


def _streamer_message(code, command="LOGIN"):
    return {
        "response": [{
            "service": "ADMIN",
            "command": command,
            "requestid": "0",
            "SchwabClientCorrelId": "synthetic-correl",
            "timestamp": 1764000000000,
            "content": {"code": code, "msg": "You've reached the maximum number of connections allowed."
                        if code == 12 else "synthetic"},
        }]
    }


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.delenv("SCHWAB_ORDER_POLL_SECONDS", raising=False)

    def reject(*_args, **_kwargs):
        raise AssertionError("No external requests")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def test_error_code_is_read_from_both_recorded_streamer_shapes():
    login_refused = UnexpectedResponseCode(_streamer_message(12), "unexpected response code: 12")
    mid_stream = UnexpectedResponse(_streamer_message(12, command="LOGIN"), "during message handling: 12")
    other = UnexpectedResponseCode(_streamer_message(3), "login denied")
    assert Schwab._schwab_stream_error_code(login_refused) == 12
    assert Schwab._schwab_stream_error_code(mid_stream) == 12
    assert Schwab._schwab_stream_error_code(other) == 3
    assert Schwab._schwab_stream_error_code(ConnectionError("closed")) is None


def test_poll_interval_has_a_floor(monkeypatch):
    assert Schwab._schwab_order_poll_seconds() == 2.0
    monkeypatch.setenv("SCHWAB_ORDER_POLL_SECONDS", "0.1")
    assert Schwab._schwab_order_poll_seconds() == 1.0
    monkeypatch.setenv("SCHWAB_ORDER_POLL_SECONDS", "5")
    assert Schwab._schwab_order_poll_seconds() == 5.0
    monkeypatch.setenv("SCHWAB_ORDER_POLL_SECONDS", "junk")
    assert Schwab._schwab_order_poll_seconds() == 2.0


class _Client:
    """A streamer client driven by a script of outcomes, one per login."""

    def __init__(self, script, counters):
        self.script = script
        self.counters = counters

    def add_account_activity_handler(self, handler):
        self.handler = handler

    async def login(self):
        self.counters["logins"] += 1
        outcome = self.script.pop(0) if self.script else "refused"
        self.outcome = outcome
        if outcome == "refused":
            raise UnexpectedResponseCode(_streamer_message(12), "unexpected response code: 12")

    async def account_activity_sub(self):
        return None

    async def handle_message(self):
        if self.outcome == "kicked":
            raise ConnectionError("another session opened for this login")
        # "ok": a healthy stream with one message per simulated 10 s.
        await asyncio.sleep(10)


def _run(monkeypatch, script, horizon_seconds):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _queues = _TRANSPORT.data_only_schwab_broker(transport, name="bot")
    broker.account_number = "fixture-account"
    broker._schwab_activity_stop = Event()
    broker._schwab_activity_wakeup = Event()
    counters = {"logins": 0, "wakeups": 0}
    broker.stream_client = _Client(script, counters)
    wakeup_set = broker._schwab_activity_wakeup.set

    def counting_set():
        counters["wakeups"] += 1
        wakeup_set()

    broker._schwab_activity_wakeup.set = counting_set
    clock = [0.0]
    sleeps = []
    real_sleep = asyncio.sleep

    async def virtual_sleep(delay):
        sleeps.append(delay)
        clock[0] += delay
        if clock[0] >= horizon_seconds:
            broker._schwab_activity_stop.set()
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", virtual_sleep)
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr("lumibot.brokers.schwab._get_stream_client_class",
                        lambda: (lambda *_a, **_k: _Client(script, counters)))
    asyncio.run(broker._run_schwab_account_activity_async())
    return broker, counters, sleeps


def test_code_12_at_login_polls_instead_of_reconnecting_in_a_loop(monkeypatch, caplog):
    with caplog.at_level(logging.DEBUG, logger="lumibot.brokers.schwab"):
        broker, counters, sleeps = _run(monkeypatch, [], horizon_seconds=1800)
    # 30 minutes with the stream always refused: one login, then one retry every 10 minutes.
    assert counters["logins"] <= 3, counters
    # Polling: one order-status wake every 2 s (bounded, never faster than 1 s).
    assert min(sleeps) >= 1.0
    assert counters["wakeups"] >= 800
    shared = [r for r in caplog.records if "account_activity_stream_shared_login" in r.getMessage()]
    assert len(shared) == 1 and shared[0].levelno == logging.WARNING
    assert "reason=code_12" in shared[0].getMessage()
    assert not [r for r in caplog.records if "account_activity_stream_disconnected" in r.getMessage()]
    assert broker._schwab_activity_stop.is_set()


def test_repeated_short_sessions_switch_to_polling_after_three(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING, logger="lumibot.brokers.schwab"):
        _broker, counters, sleeps = _run(monkeypatch, ["kicked"] * 50, horizon_seconds=600)
    # Two backoff waits (1 s, 2 s), then polling; no fight every few seconds.
    assert sleeps[:2] == [1.0, 2.0]
    assert counters["logins"] <= 4, counters
    warnings = [r.getMessage() for r in caplog.records]
    assert sum("account_activity_stream_shared_login" in m for m in warnings) == 1
    assert sum("account_activity_stream_disconnected" in m for m in warnings) == 2


def test_stream_is_taken_back_when_the_other_login_goes_away(monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="lumibot.brokers.schwab"):
        _broker, counters, _sleeps = _run(monkeypatch, ["refused", "ok"], horizon_seconds=900)
    assert counters["logins"] == 2
    messages = [r.getMessage() for r in caplog.records]
    assert any("account_activity_stream_recovered" in m for m in messages)


def test_polling_with_no_working_orders_sends_no_broker_requests():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _queues = _TRANSPORT.data_only_schwab_broker(transport, name="bot")
    before = len(transport.calls)
    broker._reconcile_active_schwab_orders()
    assert len(transport.calls) == before
