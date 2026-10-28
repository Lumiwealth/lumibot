"""Strategy-only regression contracts; real inference/fills are qualified separately."""

from datetime import date, datetime, timedelta
from types import SimpleNamespace as S
from zoneinfo import ZoneInfo

import pytest

from lumibot.entities import Asset, Order
from lumibot.example_strategies.ai_iron_condor import AIIronCondorStrategy
from lumibot.strategies._strategy import Vars


def option(strike, right, expiration=date(2026, 1, 7), symbol="SPY"):
    return Asset(symbol, asset_type="option", strike=strike, right=right, expiration=expiration)


def condor(expiration=date(2026, 1, 7), symbol="SPY"):
    return [S(asset=option(k, r, expiration, symbol), quantity=q)
            for k, r, q in [(589, "PUT", 1), (590, "PUT", -1), (600, "CALL", -1), (601, "CALL", 1)]]


@pytest.fixture
def strategy(monkeypatch):
    strategy = object.__new__(AIIronCondorStrategy)
    strategy.vars = Vars()
    strategy.__dict__["parameters"] = dict(AIIronCondorStrategy.parameters)
    strategy.now = datetime(2026, 1, 6, 11, 30, tzinfo=ZoneInfo("America/New_York"))
    strategy.close_seconds = 4.5 * 3600
    strategy.test_positions = []
    strategy.test_orders = []
    strategy.submitted = []
    strategy.calls = []
    strategy.messages = []
    strategy.spot = 599.0
    strategy.plan = {"decision": "open", "expiration": "2026-01-07", "long_put": 589.0,
                     "short_put": 590.0, "short_call": 600.0, "long_call": 601.0,
                     "reason": "Listed next-session contracts with observed prices."}
    strategy.get_datetime = lambda: strategy.now
    strategy.get_positions = lambda: list(strategy.test_positions)
    strategy.get_orders = lambda: list(strategy.test_orders)
    strategy.get_last_price = lambda asset: strategy.spot
    strategy.log_message = lambda message, **kwargs: strategy.messages.append(message)
    strategy.broker = S(get_time_to_close=lambda: strategy.close_seconds)
    monkeypatch.setattr(AIIronCondorStrategy, "portfolio_value", property(lambda self: 10000))
    monkeypatch.setattr(AIIronCondorStrategy, "cash", property(lambda self: 10000))
    monkeypatch.setattr(AIIronCondorStrategy, "is_backtesting", property(lambda self: True))

    def run(**kwargs):
        strategy.calls.append(kwargs)
        return S(parsed=strategy.plan, parse_error=None, usage={"input_tokens": 123, "output_tokens": 45})

    strategy.agent = S(run=run)
    strategy.created_agents = []
    def create(**kwargs):
        strategy.created_agents.append(kwargs)
        return strategy.agent
    # Existing examples use dictionary-style agent lookup.
    class Agents:
        def create(self, **kwargs):
            return create(**kwargs)
        def __getitem__(self, name):
            return strategy.agent
    strategy.agents = Agents()
    strategy.create_order = lambda asset, quantity, side, **kwargs: Order("hybrid", asset, quantity, side, **kwargs)

    def submit(orders, **kwargs):
        orders = orders if isinstance(orders, list) else [orders]
        for order in orders:
            order.status = "new"
        strategy.test_orders.extend(orders)
        strategy.submitted.append((orders, kwargs))
        return orders

    strategy.submit_order = submit
    strategy.cancelled = []
    strategy.cancel_order = lambda order: strategy.cancelled.append(order)
    strategy.get_quote = lambda asset: S(bid=.6 if asset.strike in (590, 600) else .4,
                                        ask=.6 if asset.strike in (590, 600) else .4, price=None)
    strategy.get_chains = lambda asset: {"Multiplier": 100, "Chains": {
        "PUT": {"2026-01-07": [589., 590.]}, "CALL": {"2026-01-07": [600., 601.]}}}
    strategy.initialize()
    return strategy


def test_two_expirations_are_each_monitored(strategy):
    strategy.test_positions = condor() + condor(date(2026, 1, 8))
    strategy.on_trading_iteration()
    assert len(strategy.submitted) == 2
    assert not strategy.calls
    assert all(len({o.asset.expiration for o in orders}) == 1 for orders, _ in strategy.submitted)


def test_unrelated_order_does_not_suppress_risk_exit(strategy):
    strategy.test_positions = condor()
    strategy.test_orders = [Order("hybrid", Asset("OTHER"), 1, "buy")]
    strategy.on_trading_iteration()
    assert len(strategy.submitted) == 1
    assert not strategy.cancelled


def test_persistent_breach_submits_once_without_any_ai_calls(strategy):
    strategy.test_positions = condor()
    for _ in range(3):
        strategy.on_trading_iteration()
    assert len(strategy.submitted) == 1
    assert not strategy.calls


def test_orphan_option_is_closed_with_correct_side(strategy):
    strategy.test_positions = [condor()[1]]
    strategy.on_trading_iteration()
    orders, kwargs = strategy.submitted[0]
    assert len(orders) == 1 and orders[0].side == "buy_to_close"


def test_other_underlying_and_cash_are_not_treated_as_condor(strategy):
    strategy.test_positions = condor(symbol="OTHER") + [S(asset=Asset("USD", asset_type="forex"), quantity=10000)]
    strategy.on_trading_iteration()
    assert not strategy.submitted and not strategy.calls


def test_close_sides_and_quantities_are_actual_positions(strategy):
    strategy.test_positions = condor()
    strategy.on_trading_iteration()
    orders, kwargs = strategy.submitted[0]
    assert kwargs["is_multileg"] is True
    assert [o.side for o in orders] == ["sell_to_close", "buy_to_close", "buy_to_close", "sell_to_close"]
    assert all(o.quantity == 1 for o in orders)


def test_pending_entry_is_cancelled_before_closing_residual(strategy):
    strategy.test_positions = [condor()[1]]
    entry = Order("hybrid", strategy.test_positions[0].asset, 2, "sell_to_open")
    strategy.test_orders = [entry]
    strategy.on_trading_iteration()
    assert strategy.cancelled == [entry] and not strategy.submitted
    entry.status = "canceled"
    strategy.on_trading_iteration()
    assert strategy.submitted[0][0][0].quantity == 1


def test_cancel_confirmation_prevents_duplicate_reprice(strategy):
    strategy.test_positions = condor()
    strategy.on_trading_iteration()
    strategy.now += timedelta(minutes=6)
    strategy.on_trading_iteration()
    assert strategy.cancelled and len(strategy.submitted) == 1
    for order in strategy.test_orders:
        order.status = "canceled"
    strategy.on_trading_iteration()
    assert len(strategy.submitted) == 2
    assert not strategy.calls


def test_expiring_condor_closes_before_next_daily_entry(strategy):
    strategy.test_positions = condor(date(2026, 1, 6))
    strategy.spot = 595
    strategy.close_seconds = 30 * 60
    strategy.on_trading_iteration()
    assert len(strategy.submitted) == 1 and not strategy.calls
    strategy.close_seconds = 15 * 60
    strategy.on_trading_iteration()
    assert not strategy.calls, "an unconfirmed close must not be followed by another entry"


def test_flat_entry_only_calls_planner_once_and_validates_risk(strategy):
    strategy.spot = 595
    strategy.close_seconds = 15 * 60
    strategy.on_trading_iteration()
    strategy.on_trading_iteration()
    assert len(strategy.calls) == 1 and len(strategy.submitted) == 1
    assert not strategy.created_agents[0]["allow_trading"]
    assert strategy.created_agents[0]["model"] == "chatgpt/gpt-6-luna"
    orders, kwargs = strategy.submitted[0]
    assert len(orders) == 4
    assert kwargs["order_type"] == "credit" and kwargs["price"] == pytest.approx(.4)
    assert (100 * (1 - .4) + 8 * .65) * float(orders[0].quantity) <= 300


def test_malformed_plan_never_creates_unprotected_entry(strategy):
    strategy.spot = 595
    strategy.close_seconds = 15 * 60
    strategy.plan["long_put"] = 591
    strategy.on_trading_iteration()
    assert not strategy.submitted and len(strategy.calls) == 1


def test_reinitializing_preserves_entry_attempt_and_pending_state(strategy):
    strategy.vars.entry_day = "2026-01-06"
    strategy.vars.packages = {"2026-01-07": {"exit_attempts": 2}}
    strategy.initialize()
    assert strategy.vars.entry_day == "2026-01-06"
    assert strategy.vars.packages["2026-01-07"]["exit_attempts"] == 2


@pytest.mark.parametrize("price,closes", [(595., False), (596.9, False), (597., True), (592.5, True)])
def test_original_price_distance_threshold_is_preserved(strategy, price, closes):
    strategy.test_positions = condor()
    strategy.spot = price
    strategy.on_trading_iteration()
    assert bool(strategy.submitted) is closes
    assert not strategy.calls


def test_rejected_close_retries_actual_remaining_positions_without_ai(strategy):
    strategy.test_positions = condor()
    strategy.on_trading_iteration()
    for order in strategy.test_orders:
        order.status = "error"
    strategy.test_positions = [condor()[1]]
    strategy.on_trading_iteration()
    orders, _ = strategy.submitted[-1]
    assert len(orders) == 1 and orders[0].quantity == 1 and orders[0].side == "buy_to_close"
    assert not strategy.calls


def test_assignment_short_stock_is_bought_back(strategy):
    strategy.vars.has_entered = True
    strategy.test_positions = [S(asset=Asset("SPY"), quantity=-100)]
    strategy.get_quote = lambda asset: S(bid=595., ask=595.1)
    strategy.on_trading_iteration()
    orders, _ = strategy.submitted[0]
    assert orders[0].side == "buy" and orders[0].quantity == 100


def test_early_close_uses_remaining_session_time_not_wall_clock(strategy):
    strategy.now = datetime(2026, 11, 27, 12, 30, tzinfo=ZoneInfo("America/New_York"))
    strategy.test_positions = condor(date(2026, 11, 27))
    strategy.close_seconds = 30 * 60
    strategy.spot = 595
    strategy.on_trading_iteration()
    assert len(strategy.submitted) == 1
    assert strategy.minutes_before_closing == 0


def test_exit_latches_when_price_recovers(strategy):
    strategy.test_positions = condor()
    strategy.on_trading_iteration()
    strategy.spot = 595
    for order in strategy.test_orders:
        order.status = "canceled"
    strategy.on_trading_iteration()
    assert len(strategy.submitted) == 2


def test_uncertain_submit_cannot_create_duplicate_exit(strategy):
    strategy.test_positions = condor()
    def uncertain(*args, **kwargs):
        raise ConnectionError("unknown broker outcome")
    strategy.submit_order = uncertain
    with pytest.raises(ConnectionError):
        strategy.on_trading_iteration()
    strategy.submit_order = lambda *args, **kwargs: pytest.fail("duplicate after uncertain submit")
    strategy.on_trading_iteration()
    assert any("exit_reconciliation_required" in message for message in strategy.messages)


def test_historical_mark_is_explicit_and_missing_quotes_do_not_invent_prices(strategy):
    strategy.test_positions = condor()
    strategy.get_quote = lambda asset: S(bid=None, ask=None, price=.1 if asset.strike in (589, 601) else .2)
    strategy.on_trading_iteration()
    assert any("historical_trade_mark_not_nbbo" in message for message in strategy.messages)
    for order in strategy.test_orders:
        order.status = "canceled"
    strategy.get_quote = lambda asset: S(bid=None, ask=None, price=None)
    strategy.on_trading_iteration()
    assert len(strategy.submitted) == 1


def test_confirmed_flat_allows_next_sessions_entry(strategy):
    strategy.spot = 595
    strategy.close_seconds = 15 * 60
    strategy.on_trading_iteration()
    for order in strategy.test_orders:
        order.status = "fill"
    strategy.test_positions = condor()
    strategy.now = datetime(2026, 1, 7, 15, 30, tzinfo=ZoneInfo("America/New_York"))
    strategy.close_seconds = 30 * 60
    strategy.on_trading_iteration()
    for order in strategy.test_orders:
        order.status = "fill"
    strategy.test_positions = []
    strategy.now += timedelta(minutes=15)
    strategy.close_seconds = 15 * 60
    strategy.plan["expiration"] = "2026-01-08"
    strategy.get_chains = lambda asset: {"Chains": {"PUT": {"2026-01-08": [589., 590.]},
                                                 "CALL": {"2026-01-08": [600., 601.]}}}
    strategy.on_trading_iteration()
    assert len(strategy.calls) == 2 and len(strategy.submitted) == 3
