"""The Schwab request pacer must hold its per-minute setting over ANY 60 seconds.

A live run with SCHWAB_REQUESTS_PER_MINUTE=100 measured 132.7 requests per minute
(92 at a setting of 80): a bucket that starts full lets a whole minute of requests
through at once and then refills at the full rate, so the first minute carries up to
twice the setting. The Schwab application limit (about 120 per minute) is shared by
every process on the same Schwab app, so one process overshooting its share can get
everyone answered with HTTP 429. No network.
"""
from collections import deque

import pytest

from lumibot.brokers.schwab_request_pacer import SchwabRequestPacer
from lumibot.tools.http_retry_after import RateLimitedError


class _Clock:
    def __init__(self):
        self.now = 0.0
        self.sent = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(1e-6, seconds)  # a real sleep always lets some time pass


def _max_in_any_minute(times):
    window, best = deque(), 0
    for stamp in times:
        window.append(stamp)
        while window and stamp - window[0] >= 60.0:
            window.popleft()
        best = max(best, len(window))
    return best


def _drive(pacer, clock, seconds, kinds):
    """Ask as fast as the pacer allows for ``seconds``; record when each request went out."""
    sent, step = [], 0
    while clock.now < seconds:
        kind = kinds[step % len(kinds)]
        step += 1
        try:
            pacer.acquire(kind)
        except RateLimitedError as exc:
            clock.sleep(max(0.01, exc.retry_after_seconds or 0.01))
            continue
        sent.append((clock.now, kind))
    return sent


@pytest.mark.parametrize("setting", [100, 80, 60])
def test_no_minute_exceeds_the_setting_from_start_up_on(setting):
    clock = _Clock()
    pacer = SchwabRequestPacer(requests_per_minute=setting, order_reserved_per_minute=20,
                               market_data_max_wait_seconds=2.0, clock=clock, sleep=clock.sleep)
    sent = _drive(pacer, clock, 300, ["data", "data", "account", "order", "data"])
    assert _max_in_any_minute([t for t, _ in sent]) <= setting
    # It is a budget, not a brake: a busy process still gets close to its share.
    # Four of every five requests here are reads, which may use the setting minus
    # the 20 per minute kept for orders.
    steady = [t for t, _ in sent if 120 <= t < 180]
    assert len(steady) >= 0.85 * min(setting, (setting - 20) * 5 / 4)


def test_market_data_alone_stays_under_the_setting_minus_the_order_reserve():
    clock = _Clock()
    pacer = SchwabRequestPacer(requests_per_minute=100, order_reserved_per_minute=20,
                               market_data_max_wait_seconds=2.0, clock=clock, sleep=clock.sleep)
    sent = _drive(pacer, clock, 240, ["data"])
    assert _max_in_any_minute([t for t, _ in sent]) <= 80


def test_orders_still_go_out_promptly_while_quotes_use_their_whole_share():
    clock = _Clock()
    pacer = SchwabRequestPacer(requests_per_minute=100, order_reserved_per_minute=20,
                               market_data_max_wait_seconds=2.0, clock=clock, sleep=clock.sleep)
    _drive(pacer, clock, 120, ["data"])
    started = clock.now
    pacer.acquire("order")
    assert clock.now - started < 1.0


def test_a_data_source_with_its_own_client_shares_the_brokers_budget():
    from types import SimpleNamespace

    from lumibot.brokers.schwab import Schwab
    from lumibot.brokers.schwab_request_pacer import pacer_for_client

    broker_client, data_client = SimpleNamespace(), SimpleNamespace()
    fake = SimpleNamespace(client=broker_client, data_source=SimpleNamespace(client=data_client),
                           _schwab_pacing_config={"SCHWAB_REQUESTS_PER_MINUTE": 100})
    Schwab._share_request_budget_with_data_source(fake)
    assert pacer_for_client(data_client) is pacer_for_client(broker_client)


def test_the_speed_summary_counts_each_request_once_however_many_paths_use_the_pacer():
    from types import SimpleNamespace

    from lumibot.brokers.schwab import Schwab
    from lumibot.brokers.schwab_request_pacer import pacer_for_client
    from lumibot.tools.speed_summary import SpeedSummary

    client = SimpleNamespace()
    recorder = SpeedSummary(interval=0)
    fake = SimpleNamespace(client=client, _schwab_pacing_config=None, _speed_summary=lambda: recorder)
    for _ in range(3):
        Schwab._schwab_pace(fake, "order")
    pacer_for_client(client).acquire("data")  # the data source path, same pacer
    snapshot = recorder.snapshot()
    assert snapshot["requests"]["total"] == 4
    assert len(pacer_for_client(client).listeners) == 1
