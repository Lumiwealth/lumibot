"""Live speed summary: one log line with iteration, request and order timings. No network."""
import json
import logging
from types import SimpleNamespace

from lumibot.brokers.schwab_request_pacer import SchwabRequestPacer
from lumibot.tools.speed_summary import SpeedSummary


class _Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def test_order_steps_are_timed_from_submit_to_fill_to_callback_to_next_order():
    clock = _Clock()
    summary = SpeedSummary(clock=clock, interval=60)
    started = summary.order_submitting()
    clock.now += 0.3
    summary.order_submitted("A", started)
    clock.now += 1.2
    summary.fill_seen("A")
    clock.now += 0.05
    callback_started = clock.now
    clock.now += 0.02
    summary.fill_callback(callback_started, clock.now)
    clock.now += 0.9
    summary.order_submitting()  # the next order, 970 ms after the fill was seen
    orders = summary.snapshot()["orders"]
    assert orders["submit_ms"]["median"] == 300
    assert orders["submit_to_fill_ms"]["median"] == 1500
    assert orders["fill_to_callback_ms"]["median"] == 50
    assert orders["callback_ms"]["median"] == 20
    assert orders["fill_to_next_order_ms"]["median"] == 970


def test_one_summary_line_per_interval_with_iterations_and_requests_per_minute(caplog):
    clock = _Clock()
    summary = SpeedSummary(clock=clock, interval=60)
    logger = logging.getLogger("speed-test")
    for seconds in (1.0, 2.0, 9.0):
        summary.iteration(seconds)
    for _ in range(30):
        summary.request("data", 0.1)
    assert summary.maybe_emit(logger) is None  # interval not over yet
    clock.now += 60
    with caplog.at_level(logging.INFO, logger="speed-test"):
        payload = summary.maybe_emit(logger)
    assert payload["iterations"]["count"] == 3 and payload["iterations"]["seconds"]["max"] == 9.0
    assert payload["requests"]["per_minute"]["data"] == 30.0
    assert payload["requests"]["budget_wait_seconds"] == 3.0
    line = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("[SpeedSummary] "))
    assert json.loads(line.split(" ", 1)[1])["schema"] == "lumibot.speed_summary/1"
    assert summary.snapshot()["iterations"]["count"] == 0  # a new window started


def test_zero_interval_turns_the_line_off(monkeypatch):
    monkeypatch.setenv("LUMIBOT_SPEED_SUMMARY_SECONDS", "0")
    clock = _Clock()
    summary = SpeedSummary(clock=clock)
    clock.now += 10_000
    assert summary.maybe_emit(logging.getLogger("x")) is None


def test_schwab_pacer_reports_each_request_and_its_wait_to_listeners():
    seen = []
    pacer = SchwabRequestPacer(requests_per_minute=100)
    pacer.listeners.append(lambda kind, waited: seen.append((kind, waited)))
    pacer.acquire("data")
    pacer.acquire("order")
    assert [kind for kind, _ in seen] == ["data", "order"]


def test_broker_submit_is_timed_in_live_and_not_in_backtests():
    from lumibot.brokers.broker import Broker

    calls = []

    class _Fake:
        IS_BACKTESTING_BROKER = False
        _speed_summary = Broker._speed_summary
        submit_order = Broker.submit_order

        def _refuse_unsupported_order(self, order):
            return False

        def resolve_option_order_intent(self, order):
            return order

        def _conform_order(self, order):
            return None

        def _submit_order(self, order):
            calls.append(order)
            return order

    live = _Fake()
    order = SimpleNamespace(identifier="X1")
    live.submit_order(order)
    assert live._speed_summary().snapshot()["orders"]["submit_ms"]["n"] == 1
    backtest = _Fake()
    backtest.IS_BACKTESTING_BROKER = True
    backtest.submit_order(order)
    assert "_speed_summary_recorder" not in backtest.__dict__
