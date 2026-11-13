"""Generic condition arithmetic and read-only diagnostics, without model calls."""
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents.attention import AttentionMonitor, observation_key
from lumibot.entities import Asset


class Store(dict):
    def set(self, key, value):
        self[key] = value


class Strategy:
    def __init__(self, store=None):
        self.vars = store if store is not None else Store()
        self.now = datetime(2026, 10, 1, 14, tzinfo=timezone.utc)
        self.positions = [SimpleNamespace(asset=Asset("XYZ"), quantity=2)]
        self.model_calls = 0

    def get_datetime(self):
        return self.now

    def get_positions(self):
        return self.positions


def test_profit_capture_watch_uses_total_cash_units():
    strategy = Strategy()
    monitor = AttentionMonitor(strategy)
    asset = {"symbol": "XYZ"}
    monitor.watch(owner="decision", watch_id="capture", operation_id="holding",
        metric="basket_pnl", comparison="gte", value=120, entry_cash=300,
        legs=[{"asset": asset, "quantity": -2, "multiplier": 100}],
        reason="Review after capturing forty percent of the total opening credit")
    # $300 opening cash less the total remaining liability; per-share price is
    # only an observation input and is never the P&L threshold's unit.
    assert monitor.poll(observations={observation_key("price", asset): 1.0}) == []
    event = monitor.poll(observations={observation_key("price", asset): 0.9})[0]
    assert event["observed"] == 120
    assert strategy.model_calls == 0


@pytest.mark.parametrize("cash,threshold,percent", [(300, 12, 4), (-300, -120, -40), (0, 12, None)])
def test_pnl_preview_reports_the_selected_currency_threshold_basis(cash, threshold, percent):
    strategy = Strategy()
    monitor = AttentionMonitor(strategy)
    definition = monitor.watch(owner="decision", watch_id="review", operation_id="holding",
        metric="basket_pnl", comparison="gte", value=threshold, entry_cash=cash,
        legs=[{"asset": {"symbol": "XYZ"}, "quantity": -2, "multiplier": 100}],
        reason="Inspect the units of the selected boundary before finishing")
    before = deepcopy(strategy.vars)
    # The percentage meaning of the selected threshold remains available even
    # when the price needed to observe current P&L is unavailable.
    preview = monitor.preview(definition)
    assert preview["value_units"] == "currency"
    assert preview["threshold_percent_of_absolute_entry_cash"] == percent
    assert strategy.vars == before
    assert monitor.pending() == []
    assert strategy.model_calls == 0


@pytest.mark.parametrize("cash,quantities,initial,worse,expected", [
    (30, [-1, 1], [0.6, 0.3], [0.9, 0.3], -30),
    (-30, [1, -1], [0.6, 0.3], [0.3, 0.3], -30),
    (-90, [2, -1], [0.6, 0.3], [0.45, 0.3], -30),
    (29, [-1, 1], [0.6, 0.3], [0.9, 0.3], -31),
])
def test_pnl_condition_uses_selected_cash_basis_without_a_model(cash, quantities, initial, worse, expected):
    strategy = Strategy(); monitor = AttentionMonitor(strategy)
    assets = [{"symbol": "XYZ"}, {"symbol": "ABC"}]
    legs = [{"asset": asset, "quantity": quantity, "multiplier": 100}
            for asset, quantity in zip(assets, quantities)]
    watch = monitor.watch(owner="decision", watch_id="risk", operation_id="holding",
        metric="basket_pnl", comparison="lte", value=-20, entry_cash=cash,
        legs=legs, reason="Agent-selected currency loss boundary")
    assert watch["entry_cash"] == cash
    observations = lambda prices: {observation_key("price", asset): price for asset, price in zip(assets, prices)}
    for _ in range(20):
        assert monitor.poll(observations=observations(initial)) == []
    event = monitor.poll(observations=observations(worse))[0]
    assert event["observed"] == pytest.approx(expected)
    monitor.acknowledge([event["id"]])
    restarted = AttentionMonitor(Strategy(strategy.vars))
    assert restarted.snapshot(owner="decision")["watches"][0]["entry_cash"] == cash
    assert restarted.poll(observations=observations(worse)) == []
    assert strategy.model_calls == 0


@pytest.mark.parametrize("cash", [None, float("nan"), float("inf"), True])
def test_pnl_condition_requires_an_explicit_finite_selected_cash_basis(cash):
    with pytest.raises(ValueError, match="cash"):
        AttentionMonitor(Strategy()).watch(owner="decision", watch_id="risk", operation_id="holding",
            metric="basket_pnl", comparison="lte", value=-20, entry_cash=cash,
            legs=[{"asset": {"symbol": "XYZ"}, "quantity": -1, "multiplier": 100}],
            reason="Agent-selected loss boundary")


def test_condition_preview_exposes_an_already_true_holding_warning_without_dispatch_or_acknowledgment():
    strategy = Strategy(); monitor = AttentionMonitor(strategy)
    watch = monitor.watch(owner="decision", watch_id="residual", operation_id="holding",
        metric="position_quantity", comparison="ne", value=0, asset={"symbol": "XYZ"},
        reason="Inspect whether the selected residual condition is already true")
    before = deepcopy(strategy.vars)
    preview = monitor.preview(watch)
    assert preview["available"] is True
    assert preview["observed"] == 2 and preview["condition_met"] is True
    assert strategy.vars == before and monitor.pending() == []
    assert strategy.model_calls == 0
    assert len(monitor.poll()) == 1


def test_unavailable_preview_does_not_remove_or_acknowledge_its_condition():
    strategy = Strategy(); monitor = AttentionMonitor(strategy)
    watch = monitor.watch(owner="decision", watch_id="price", operation_id="holding",
        metric="price", comparison="lte", value=10, asset={"symbol": "XYZ"},
        reason="Retain this condition even if the current observation is unavailable")
    before = deepcopy(strategy.vars)
    preview = monitor.preview(watch)
    assert preview["available"] is False
    assert strategy.vars == before and monitor.snapshot(owner="decision")["watches"] == [watch]


def test_model_tool_preserves_selected_cash_basis_and_returns_read_only_feedback():
    from lumibot.components.agents.builtins import _bind_attention_watch
    from lumibot.components.agents.tool_context import agent_tool_context
    strategy = Strategy()
    strategy.get_last_price = lambda asset: 0.4
    manager = SimpleNamespace(attention=AttentionMonitor(strategy))
    tool = _bind_attention_watch(strategy, manager)
    with agent_tool_context({"agent_name": "decision"}):
        result = tool.function(watch_id="risk", operation_id="holding", metric="basket_pnl",
            comparison="lte", value_json="-20", entry_cash=-90,
            legs_json='[{"asset":{"symbol":"XYZ"},"quantity":2,"multiplier":100}]',
            reason="Selected loss review")
    assert result["entry_cash"] == -90
    assert result["registration_observation"]["observed"] == -10
    assert result["registration_observation"]["condition_met"] is False
    assert result["registration_observation"]["value_units"] == "currency"
    assert result["registration_observation"]["threshold_percent_of_absolute_entry_cash"] == pytest.approx(-20 / 90 * 100)
    assert manager.attention.pending() == []
    assert manager.attention.snapshot(owner="decision")["watches"][0]["entry_cash"] == -90
