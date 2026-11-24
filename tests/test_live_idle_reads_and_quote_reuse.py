"""Live request savings and their real-money safety rules. No network, no broker writes.

A live scanner on Schwab used to read the whole account order list and the account
on every pass, ask for each quote separately and fetch every option chain on every
pass. This file proves the savings AND the rules that keep them safe:

* no order-list skip while any order is UNKNOWN or still working, while a submit is
  unresolved, or for a while after an order was canceled (late fills);
* a full read at least every LUMIBOT_IDLE_SYNC_MAX_AGE seconds, and before any order
  action that follows a skipped read;
* quotes are reused only while younger than LUMIBOT_QUOTE_MAX_AGE, never when absent,
  and assets Schwab cannot quote never enter the prefetch list;
* option chains are handed out as copies, and today's expiry drops after the close;
* waking the iteration after a fill is opt-in, partial fills only when asked.
"""
import datetime as dt
import time as _time
from types import SimpleNamespace

import pytest

from lumibot.brokers.broker import Broker
from lumibot.data_sources.schwab_data import SchwabData
from lumibot.entities import Asset, Order
from tests.test_schwab_lifecycle_transport import drain_stream_events, stock, submitted

import lumibot.brokers.broker as broker_module
import lumibot.data_sources.schwab_data as schwab_data_module


class _Clock:
    def __init__(self):
        self.now = 10_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    fake = _Clock()
    monkeypatch.setattr(broker_module.time, "monotonic", fake)
    return fake


def _list_reads(transport):
    return sum(1 for call in transport.calls if call["method"] == "get_orders_for_account")


def _read_everything(broker):
    """One good order-list read plus one good account read, as a live sync does."""
    broker.sync_orders("fixture")
    broker._note_account_read_ok(broker._order_activity_epoch)


# ------------------------------------------------------------------ idle order reads

def test_reads_are_skipped_only_when_nothing_can_change(clock):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED
    _read_everything(broker)
    clock.now += 5
    assert broker.idle_read_skip_age("fixture") == pytest.approx(5)


@pytest.mark.parametrize(
    "status",
    [Order.OrderStatus.UNKNOWN, Order.OrderStatus.NEW, Order.OrderStatus.SUBMITTED, Order.OrderStatus.OPEN,
     Order.OrderStatus.PARTIALLY_FILLED, Order.OrderStatus.CANCELLING],
)
def test_no_skip_while_any_order_is_unknown_or_working(clock, status):
    transport, broker, _queues, current = submitted()
    _read_everything(broker)
    current.status = status
    assert broker.idle_read_skip_age("fixture") is None


def test_a_canceled_order_keeps_reads_going_so_a_late_fill_is_caught(clock):
    transport, broker, queues, current = submitted()
    current.status = Order.OrderStatus.CANCELED
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is None  # late fill window
    # Schwab reports the canceled order as filled after all: the next read sees it.
    late = dict(transport._latest[current.identifier], status="FILLED", filledQuantity=100)
    late["orderActivityCollection"] = [{
        "activityType": "EXECUTION", "executionType": "FILL", "quantity": 100,
        "executionLegs": [{"legId": 1, "quantity": 100, "price": 10.0, "time": "2026-11-24T15:00:00+0000"}],
    }]
    transport.queue_history_response([late])
    broker.sync_orders("fixture")
    drain_stream_events(broker)
    assert current.is_filled() or current.status in (Order.OrderStatus.FILLED, "fill")
    # After the watch window an ended order no longer blocks the skip.
    current.status = Order.OrderStatus.CANCELED
    _read_everything(broker)
    clock.now += broker.ended_order_watch_seconds + 1
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is not None


@pytest.mark.parametrize("status_code", [500, 503])
def test_a_submit_without_a_broker_id_keeps_reads_going(clock, status_code):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED
    transport.queue_submission_response(status_code, "upstream failure")
    assert broker.submit_order(stock(10)) is None  # no broker order id came back
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is None
    clock.now += broker.unresolved_submit_watch_seconds + 1
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is not None


def test_a_submit_that_times_out_keeps_reads_going(clock):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED

    def boom(*_args, **_kwargs):
        raise TimeoutError("read timed out")

    transport.place_order = boom
    broker.submit_order(stock(10))
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is None


def test_a_cleanly_refused_submit_does_not_block_the_skip(clock):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED
    transport.queue_submission_response(400, "buying power")
    broker.submit_order(stock(10))
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is not None


def test_full_read_backstop_and_off_switch(clock, monkeypatch):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED
    _read_everything(broker)
    clock.now += 29
    assert broker.idle_read_skip_age("fixture") is not None
    clock.now += 2
    assert broker.idle_read_skip_age("fixture") is None  # older than 30 s: read again
    monkeypatch.setenv("LUMIBOT_IDLE_SYNC_MAX_AGE", "0")
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is None


def test_order_activity_and_account_stream_events_end_the_skip(clock):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED
    _read_everything(broker)
    broker._handle_schwab_account_activity({"content": [{"MESSAGE_TYPE": "OrderEntryRequest"}]})
    assert broker.idle_read_skip_age("fixture") is None
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is not None


def test_an_order_action_after_a_skipped_read_reads_the_order_list_first(clock):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED
    _read_everything(broker)
    assert broker.idle_read_skip_age("fixture") is not None  # a read was served from memory
    before = len(transport.calls)
    broker.submit_order(stock(10))
    methods = [call["method"] for call in transport.calls[before:]]
    assert methods[:2] == ["get_orders_for_account", "place_order"]


def test_no_extra_read_before_an_order_action_when_nothing_was_skipped(clock):
    transport, broker, _queues, current = submitted()
    before = len(transport.calls)
    broker.submit_order(stock(10))
    assert [call["method"] for call in transport.calls[before:]] == ["place_order"]


def test_a_broker_that_never_stamps_reads_never_skips():
    fake = SimpleNamespace(IS_BACKTESTING_BROKER=False, _held_trades=[], _order_activity_epoch=0,
                           _strategy_name_from_input=lambda s: s, get_tracked_orders=lambda name: [],
                           _idle_sync_max_age=Broker._idle_sync_max_age)
    assert Broker.idle_read_skip_age(fake, "s") is None


# ------------------------------------------------------------------ quote cache

def _quote_source(monkeypatch, payload=None):
    now = {"t": 1_000.0}
    calls = []

    def get_quotes(symbols):
        calls.append(list(symbols))
        body = payload(symbols) if payload else {s: {"quote": {"bidPrice": 1.0, "askPrice": 1.1}} for s in symbols}
        return SimpleNamespace(status_code=200, json=lambda: body)

    data = SchwabData(client=SimpleNamespace(get_quotes=get_quotes), auto_create_client=False)
    monkeypatch.setattr(data, "_cache_clock", lambda: now["t"])
    return data, calls, now


def test_a_second_pass_of_a_one_at_a_time_scan_is_one_request(monkeypatch):
    data, calls, now = _quote_source(monkeypatch)
    basket = [Asset(f"S{i}") for i in range(20)]
    for asset in basket:
        data.get_quote(asset)
    assert len(calls) == 20
    now["t"] += 5  # next pass: everything older than the max age
    assert data.prefetch_quotes() == 1
    for asset in basket:
        quote = data.get_quote(asset)
        assert quote.cache_age_seconds == 0.0
    assert len(calls) == 21


def test_a_quote_older_than_the_max_age_is_fetched_again(monkeypatch):
    data, calls, now = _quote_source(monkeypatch)
    data.get_quote(Asset("ABC"))
    now["t"] += 0.4
    assert data.get_quote(Asset("ABC")).cache_age_seconds == pytest.approx(0.4)
    now["t"] += 0.7
    data.get_quote(Asset("ABC"))
    assert len(calls) == 2


def test_max_age_zero_turns_cache_and_prefetch_off(monkeypatch):
    monkeypatch.setenv("LUMIBOT_QUOTE_MAX_AGE", "0")
    data, calls, _now = _quote_source(monkeypatch)
    data.get_quote(Asset("ABC"))
    data.get_quote(Asset("ABC"))
    assert data.prefetch_quotes() == 0
    assert len(calls) == 2


def test_an_absent_quote_is_never_kept(monkeypatch):
    data, calls, _now = _quote_source(monkeypatch, payload=lambda symbols: {})
    assert data.get_quote(Asset("GONE")) is None
    assert data.get_quote(Asset("GONE")) is None
    assert len(calls) == 2  # asked again, not served a remembered "nothing"


def test_assets_schwab_cannot_quote_never_enter_the_prefetch(monkeypatch):
    data, calls, now = _quote_source(monkeypatch)
    assert data.get_quote(Asset("BTC", asset_type=Asset.AssetType.CRYPTO)) is None
    data.get_quote(Asset("ABC"))
    now["t"] += 5
    data.prefetch_quotes()
    assert calls[-1] == ["ABC"]


def test_a_failed_prefetch_drops_its_batch_and_never_raises(monkeypatch):
    data, calls, now = _quote_source(monkeypatch)
    data.get_quote(Asset("ABC"))
    now["t"] += 5
    monkeypatch.setattr(data, "_fetch_quotes_uncached", lambda *a, **k: (_ for _ in ()).throw(ConnectionError("x")))
    assert data.prefetch_quotes() == 1
    assert data.prefetch_quotes() == 0  # the failed batch left the list


def test_prefetch_forgets_assets_not_asked_for_in_the_last_two_passes(monkeypatch):
    data, calls, now = _quote_source(monkeypatch)
    data.get_quote(Asset("OLD"))
    for _ in range(3):
        now["t"] += 2
        data.prefetch_quotes()
    assert len(calls) == 3  # two prefetches, then OLD was dropped
    assert data.prefetch_quotes() == 0


def test_large_prefetch_uses_one_request_per_250_symbols(monkeypatch):
    data, calls, now = _quote_source(monkeypatch)
    data.get_quotes([Asset(f"S{i}") for i in range(300)])
    now["t"] += 5
    calls.clear()
    assert data.prefetch_quotes() == 2
    assert sorted(len(batch) for batch in calls) == [50, 250]


# ------------------------------------------------------------------ option chains

def _chain_source(monkeypatch, expiries=("2026-11-24", "2026-11-27")):
    now = {"t": 1_000.0}
    fetched = []
    data = SchwabData(client=SimpleNamespace(), auto_create_client=False)
    monkeypatch.setattr(data, "_cache_clock", lambda: now["t"])

    def uncached(asset, **_kwargs):
        fetched.append(asset.symbol)
        return {"Multiplier": 100, "Chains": {"CALL": {e: [10.0, 11.0] for e in expiries}, "PUT": {}}}

    monkeypatch.setattr(data, "_get_chains_uncached", uncached)
    return data, fetched, now


def test_a_live_chain_is_fetched_once_per_window_and_handed_out_as_a_copy(monkeypatch):
    data, fetched, now = _chain_source(monkeypatch)
    data._new_york_clock = lambda: dt.datetime(2026, 11, 24, 10, 0)
    first = data.get_chains(Asset("ABC"))
    first["Chains"]["CALL"]["2026-11-27"].append(99.0)  # a strategy edits its copy
    first["Chains"]["CALL"].pop("2026-11-24")
    second = data.get_chains(Asset("ABC"))
    assert fetched == ["ABC"]
    assert second["Chains"]["CALL"] == {"2026-11-24": [10.0, 11.0], "2026-11-27": [10.0, 11.0]}
    now["t"] += 901
    data.get_chains(Asset("ABC"))
    assert fetched == ["ABC", "ABC"]


def test_after_the_close_a_kept_chain_no_longer_lists_todays_expiry(monkeypatch):
    data, fetched, _now = _chain_source(monkeypatch)
    data._new_york_clock = lambda: dt.datetime(2026, 11, 24, 15, 59)
    data.get_chains(Asset("ABC"))
    data._new_york_clock = lambda: dt.datetime(2026, 11, 24, 16, 0)
    chain = data.get_chains(Asset("ABC"))
    assert list(chain["Chains"]["CALL"]) == ["2026-11-27"]


def test_chain_cache_can_be_turned_off(monkeypatch):
    monkeypatch.setenv("LUMIBOT_CHAINS_CACHE_SECONDS", "0")
    data, fetched, _now = _chain_source(monkeypatch)
    data.get_chains(Asset("ABC"))
    data.get_chains(Asset("ABC"))
    assert fetched == ["ABC", "ABC"]


# ------------------------------------------------------------------ wake after a fill

def _executor_with_scheduler(in_iteration=False, wake=True, wake_partial=False):
    import logging

    from apscheduler.jobstores.memory import MemoryJobStore
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger

    from lumibot.strategies.strategy_executor import StrategyExecutor

    ran = []
    scheduler = BackgroundScheduler(jobstores={"On_Trading_Iteration": MemoryJobStore()})
    scheduler.start()
    scheduler.add_job(lambda: ran.append(1), CronTrigger(minute="*"), id="OTIM", jobstore="On_Trading_Iteration")
    fake = SimpleNamespace(
        broker=SimpleNamespace(IS_BACKTESTING_BROKER=False),
        strategy=SimpleNamespace(wake_iteration_on_fill=wake, wake_iteration_on_partial_fill=wake_partial,
                                 logger=logging.getLogger("t")),
        _in_trading_iteration=in_iteration, _run_once_requested=False,
        scheduler=scheduler, cron_count=0, cron_count_target=1,
    )
    fake._is_running_once = lambda: False
    return StrategyExecutor._wake_iteration_after_fill.__get__(fake), scheduler, ran


def _wait_for(condition, seconds=3.0):
    end = _time.monotonic() + seconds
    while _time.monotonic() < end and not condition():
        _time.sleep(0.02)
    return condition()


def test_wake_after_a_fill_is_off_by_default():
    from lumibot.strategies.strategy import Strategy

    assert Strategy.wake_iteration_on_fill is False
    assert Strategy.wake_iteration_on_partial_fill is False


def test_a_strategy_that_opted_in_runs_its_next_iteration_at_once_after_a_fill():
    wake, scheduler, ran = _executor_with_scheduler()
    try:
        wake()
        assert _wait_for(lambda: ran)
    finally:
        scheduler.shutdown(wait=False)


@pytest.mark.parametrize(
    "in_iteration,wake_on,partial,wake_partial",
    [(True, True, False, False), (False, False, False, False), (False, True, True, False)],
)
def test_no_wake_while_running_when_not_opted_in_or_on_a_partial_fill(in_iteration, wake_on, partial, wake_partial):
    wake, scheduler, ran = _executor_with_scheduler(in_iteration=in_iteration, wake=wake_on, wake_partial=wake_partial)
    try:
        wake(partial=partial)
        assert not _wait_for(lambda: ran, seconds=0.5)
    finally:
        scheduler.shutdown(wait=False)


def test_partial_fills_wake_when_asked():
    wake, scheduler, ran = _executor_with_scheduler(wake_partial=True)
    try:
        wake(partial=True)
        assert _wait_for(lambda: ran)
    finally:
        scheduler.shutdown(wait=False)


def test_skipped_ticks_of_a_multi_minute_sleeptime_do_not_leave_the_iteration_flag_set():
    """With sleeptime "5M" the scheduler ticks every minute and four of five ticks only
    count. Each of those used to return with _in_trading_iteration still True, so a
    wake after a fill never fired and the queue poll stayed on its busy interval."""
    from lumibot.strategies.strategy_executor import StrategyExecutor

    fake = SimpleNamespace(
        strategy=SimpleNamespace(is_backtesting=False, _update_portfolio_value=lambda: None,
                                 _copy_dict=lambda: {}),
        broker=SimpleNamespace(IS_BACKTESTING_BROKER=False),
        cron_count=0, cron_count_target=5, _in_trading_iteration=False,
        should_continue=True, _capture_locals=False,
    )
    fake._on_trading_iteration_body = StrategyExecutor._on_trading_iteration_body.__get__(fake)
    inner = StrategyExecutor._on_trading_iteration.__wrapped__.__wrapped__
    inner(fake)
    assert fake.cron_count == 1
    assert fake._in_trading_iteration is False


def test_activity_during_an_order_list_read_means_that_read_does_not_count(clock):
    transport, broker, _queues, current = submitted()
    current.status = Order.OrderStatus.FILLED
    epoch_before = broker._order_activity_epoch
    broker._note_order_activity()  # a fill lands while the list request is in flight
    broker._note_orders_list_ok(epoch_before)
    broker._note_account_read_ok(epoch_before)
    assert broker.idle_read_skip_age("fixture") is None
