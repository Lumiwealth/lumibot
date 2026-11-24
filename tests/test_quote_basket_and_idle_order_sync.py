"""Live quote baskets and idle order-list reads. No network, no broker writes.

A strategy that quotes many assets one at a time in each iteration should cost
one quote request per iteration on a broker with a multi-symbol quote endpoint,
not one per asset. A strategy with no open orders should not read the broker's
whole order list every iteration.
"""
from datetime import date
from types import SimpleNamespace

import pytest

from lumibot.data_sources.schwab_data import SchwabData
from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _schwab_source(calls):
    def get_quotes(symbols):
        calls.append(list(symbols))
        payload = {symbol: {"quote": {"bidPrice": 1.0, "askPrice": 1.1, "lastPrice": 1.05}} for symbol in symbols}
        return SimpleNamespace(status_code=200, json=lambda: payload)

    return SchwabData(client=SimpleNamespace(get_quotes=get_quotes), auto_create_client=False)


def _strategy(source, clock, max_age=1.0):
    strategy = SimpleNamespace(
        _sanitize_user_asset=lambda asset: Asset(asset) if isinstance(asset, str) else asset,
        broker=SimpleNamespace(data_source=source, option_source=source),
        is_backtesting=False,
        quote_basket_max_age_seconds=max_age,
        _quote_basket_clock=clock,
        _quote_basket_iteration=0,
        log_message=lambda *args, **kwargs: None,
    )
    for name in ("_quote_basket_lookup", "_quote_basket_max_age", "_quote_basket_store", "_quote_basket_state"):
        setattr(strategy, name, getattr(Strategy, name).__get__(strategy))
    return strategy


def _iteration(strategy, assets):
    strategy._quote_basket_iteration += 1
    return [Strategy.get_quote(strategy, asset) for asset in assets]


def test_second_iteration_of_a_one_at_a_time_scan_is_one_quote_request():
    calls, clock = [], _Clock()
    strategy = _strategy(_schwab_source(calls), clock)
    basket = [Asset(f"S{i}") for i in range(17)]
    _iteration(strategy, basket)
    first = len(calls)
    clock.now += 5
    quotes = _iteration(strategy, basket)
    assert first == 17  # nothing to learn from yet
    assert len(calls) - first == 1
    assert sorted(calls[-1]) == sorted(asset.symbol for asset in basket)
    assert [q.asset for q in quotes] == basket


def test_quotes_older_than_the_max_age_are_fetched_again_for_the_rest_only():
    calls, clock = [], _Clock()
    strategy = _strategy(_schwab_source(calls), clock)
    basket = [Asset("A"), Asset("B"), Asset("C")]
    _iteration(strategy, basket)
    calls.clear()
    clock.now += 5
    strategy._quote_basket_iteration += 1
    Strategy.get_quote(strategy, basket[0])
    clock.now += 1.5  # older than one second
    Strategy.get_quote(strategy, basket[1])
    Strategy.get_quote(strategy, basket[2])
    assert calls == [["A", "B", "C"], ["B", "C"]]


def test_a_cached_quote_is_never_reused_in_the_next_iteration():
    calls, clock = [], _Clock()
    strategy = _strategy(_schwab_source(calls), clock)
    _iteration(strategy, [Asset("A")])
    _iteration(strategy, [Asset("A")])
    assert calls == [["A"], ["A"]]


def test_options_and_stocks_share_the_batch():
    calls, clock = [], _Clock()
    strategy = _strategy(_schwab_source(calls), clock)
    option = Asset("ABC", asset_type="option", expiration=date(2026, 11, 27), strike=12.5, right="CALL")
    _iteration(strategy, [Asset("ABC"), option])
    calls.clear()
    clock.now += 5
    _iteration(strategy, [Asset("ABC"), option])
    assert len(calls) == 1 and len(calls[0]) == 2


def test_zero_max_age_turns_the_basket_off():
    calls, clock = [], _Clock()
    strategy = _strategy(_schwab_source(calls), clock, max_age=0)
    _iteration(strategy, [Asset("A"), Asset("B")])
    clock.now += 5
    _iteration(strategy, [Asset("A"), Asset("B")])
    assert calls == [["A"], ["B"], ["A"], ["B"]]


def test_backtesting_never_uses_the_basket():
    calls, clock = [], _Clock()
    strategy = _strategy(_schwab_source(calls), clock)
    strategy.is_backtesting = True
    _iteration(strategy, [Asset("A"), Asset("B")])
    _iteration(strategy, [Asset("A"), Asset("B")])
    assert len(calls) == 4


def test_a_failed_batch_falls_back_to_the_single_quote_contract():
    calls, clock = [], _Clock()
    source = _schwab_source(calls)
    strategy = _strategy(source, clock)
    _iteration(strategy, [Asset("A"), Asset("B")])

    def broken(*args, **kwargs):
        raise ConnectionError("Schwab quote request failed (status 500)")

    source.get_quotes = broken
    single = []
    source.get_quote = lambda asset, quote=None, exchange=None: single.append(asset) or None
    clock.now += 5
    assert _iteration(strategy, [Asset("A")]) == [None]
    assert single == [Asset("A")]


def test_quote_basket_is_documented_and_on_by_default():
    assert Strategy.quote_basket_max_age_seconds == 1.0
    assert "quote_basket_max_age_seconds" in (Strategy.get_quote.__doc__ or "")


# ---------------------------------------------------------------- order lists

from tests.test_schwab_lifecycle_transport import submitted  # noqa: E402


def _order_list_reads(transport):
    return sum(1 for call in transport.calls if call["method"] == "get_orders_for_account")


def test_no_order_list_read_when_the_strategy_has_no_open_orders():
    transport, broker, queues, current = submitted()
    broker.sync_orders("fixture")
    current.status = Order.OrderStatus.FILLED
    before = _order_list_reads(transport)
    broker.sync_orders("fixture")
    broker.sync_orders("fixture")
    assert _order_list_reads(transport) == before


def test_order_list_is_still_read_while_an_order_is_open():
    transport, broker, queues, current = submitted()
    broker.sync_orders("fixture")
    before = _order_list_reads(transport)
    broker.sync_orders("fixture")
    assert _order_list_reads(transport) == before + 1


def test_order_list_read_when_idle_can_be_turned_back_on():
    transport, broker, queues, current = submitted()
    broker.skip_order_list_when_idle = False
    broker.sync_orders("fixture")
    current.status = Order.OrderStatus.FILLED
    before = _order_list_reads(transport)
    broker.sync_orders("fixture")
    assert _order_list_reads(transport) == before + 1


def test_a_canceled_order_keeps_the_order_list_in_use_for_late_fills_then_stops():
    transport, broker, queues, current = submitted()
    broker.sync_orders("fixture")
    current.status = Order.OrderStatus.CANCELED
    before = _order_list_reads(transport)
    broker.sync_orders("fixture")  # a late fill may still arrive
    assert _order_list_reads(transport) == before + 1
    broker.ended_order_watch_seconds = 0
    broker.sync_orders("fixture")
    assert _order_list_reads(transport) == before + 1


# ---------------------------------------------------------------- wake after a fill

def _executor_with_scheduler(in_iteration=False, wake=True):
    import logging
    from apscheduler.jobstores.memory import MemoryJobStore
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from lumibot.strategies.strategy_executor import StrategyExecutor

    ran = []
    scheduler = BackgroundScheduler(jobstores={"On_Trading_Iteration": MemoryJobStore()})
    scheduler.start()
    # A trigger that would not fire for the next minute on its own.
    scheduler.add_job(lambda: ran.append(1), CronTrigger(minute="*"), id="OTIM", jobstore="On_Trading_Iteration")
    fake = SimpleNamespace(
        broker=SimpleNamespace(IS_BACKTESTING_BROKER=False),
        strategy=SimpleNamespace(wake_iteration_on_fill=wake, logger=logging.getLogger("t")),
        _in_trading_iteration=in_iteration, _run_once_requested=False,
        scheduler=scheduler, cron_count=0, cron_count_target=1,
    )
    fake._is_running_once = lambda: False
    return StrategyExecutor._wake_iteration_after_fill.__get__(fake), scheduler, ran


def _wait_for(condition, seconds=3.0):
    import time as _time
    end = _time.monotonic() + seconds
    while _time.monotonic() < end and not condition():
        _time.sleep(0.02)
    return condition()


def test_a_fill_runs_the_next_iteration_at_once_instead_of_after_the_sleeptime():
    """Real Schwab run Nov 24: 905 ms of the fill-to-next-order gap was waiting for the next
    1-second tick; a strategy that hedges in its next iteration paid it on every fill."""
    wake, scheduler, ran = _executor_with_scheduler()
    try:
        wake()
        assert _wait_for(lambda: ran)
    finally:
        scheduler.shutdown(wait=False)


@pytest.mark.parametrize("in_iteration,wake_on", [(True, True), (False, False)])
def test_no_wake_during_a_running_iteration_or_when_turned_off(in_iteration, wake_on):
    wake, scheduler, ran = _executor_with_scheduler(in_iteration=in_iteration, wake=wake_on)
    try:
        wake()
        assert not _wait_for(lambda: ran, seconds=0.5)
    finally:
        scheduler.shutdown(wait=False)


# ---------------------------------------------------------------- option chain cache

def _chain_strategy(backtesting=False, ttl=900.0):
    calls = []

    def get_chains(asset):
        calls.append(asset.symbol)
        return {"Chains": {"CALL": {"2026-11-27": [10.0]}}} if asset.symbol != "EMPTY" else {}

    strategy = SimpleNamespace(
        _sanitize_user_asset=lambda asset: Asset(asset) if isinstance(asset, str) else asset,
        broker=SimpleNamespace(get_chains=get_chains, IS_BACKTESTING_BROKER=backtesting),
        is_backtesting=backtesting, chain_cache_seconds=ttl,
    )
    strategy._chain_cache_seconds = Strategy._chain_cache_seconds.__get__(strategy)
    return strategy, calls


def test_live_option_chain_is_fetched_once_and_reused_by_every_pass():
    """Replay of a 17-stock scanner Nov 24: after quote batching, option chain reads were 207 of
    337 Schwab requests in one run (the chain list changes rarely; prices come from quotes)."""
    strategy, calls = _chain_strategy()
    for _ in range(5):
        Strategy.get_chains(strategy, "ABC")
    assert calls == ["ABC"]


@pytest.mark.parametrize("backtesting,ttl,symbol", [(True, 900.0, "ABC"), (False, 0, "ABC"), (False, 900.0, "EMPTY")])
def test_chain_cache_is_off_in_backtests_when_zero_and_for_empty_answers(backtesting, ttl, symbol):
    strategy, calls = _chain_strategy(backtesting=backtesting, ttl=ttl)
    Strategy.get_chains(strategy, symbol)
    Strategy.get_chains(strategy, symbol)
    assert len(calls) == 2
