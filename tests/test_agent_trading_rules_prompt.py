"""The rebalancing and cash rules every trading agent gets from LumiBot itself.

These rules used to live in an example-only helper, so a strategy written from
scratch never got them and the examples needed a hidden import. Each rule came
from a real failed backtest: cash went negative, the trader churned small
trades, or a limit at the stale open price never filled.
"""

from datetime import datetime, timezone

from lumibot.components.agents import AgentManager


class _Vars(dict):
    def get(self, key, default=None):
        return super().get(key, default)

    def set(self, key, value):
        self[key] = value


class _Strategy:
    is_backtesting = True
    parameters = {}
    vars = _Vars()

    def get_datetime(self):
        return datetime(2026, 1, 2, tzinfo=timezone.utc)

    def log_message(self, *args, **kwargs):
        return None


def _prompt(allow_trading: bool) -> str:
    agent = AgentManager(_Strategy()).create(name="agent", allow_trading=allow_trading)
    return " ".join(agent._base_system_prompt(agent._runtime_context()).split())


def test_trading_agent_gets_the_rebalance_and_cash_rules():
    prompt = _prompt(allow_trading=True)

    assert "Plan every order from one read of the account before submitting any of them." in prompt
    assert "Leave a holding alone when it is within 2 percentage points of its target weight." in prompt
    assert "Never buy and sell the same symbol in the same session." in prompt
    assert "with about 1% left over" in prompt
    assert "Never let cash go negative" in prompt
    assert "use a market order or a limit slightly past the current price" in prompt
    assert "size to the risk target or the contract cap, whichever is smaller" in prompt
    assert "If you submit no order, no trade happens." in prompt


def test_research_agent_does_not_get_order_rules():
    prompt = _prompt(allow_trading=False)

    assert "Plan every order from one read of the account" not in prompt
    assert "You cannot place orders." in prompt
