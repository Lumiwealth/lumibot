from datetime import datetime
from unittest.mock import patch

from lumibot.example_strategies.ai_vwap import build_vwap_system_prompt
from lumibot.example_strategies.proof_modes import minute_proof_round_trip, submit_multileg_proof


class _Iloc:
    def __getitem__(self, idx):
        return 101.25


class _Series:
    iloc = _Iloc()


class _Frame:
    empty = False

    def __getitem__(self, key):
        assert key == "close"
        return _Series()


class _Bars:
    df = _Frame()


class _MinuteStrategy:
    def __init__(self):
        self.calls = []
        self.orders = []

    def get_historical_prices(self, symbol, length, timestep, include_after_hours=True):
        self.calls.append((symbol, length, timestep, include_after_hours))
        return _Bars()

    def get_last_price(self, symbol):
        raise AssertionError("minute proof must read a minute bar")

    def log_message(self, message):
        return None

    def create_order(self, symbol, qty, side):
        return (symbol, qty, side)

    def submit_order(self, order):
        self.orders.append(order)


def test_minute_proof_reads_a_minute_bar():
    strategy = _MinuteStrategy()
    minute_proof_round_trip(strategy, "SPY")
    assert strategy.calls == [("SPY", 2, "minute", False)]
    assert strategy.orders == [("SPY", 1, "buy")]


class _MultilegTool:
    def __init__(self):
        self.calls = []

    def function(self, legs_json, price_style):
        self.calls.append((legs_json, price_style))
        return {"submitted": [1]}


class _MultilegClock:
    def __init__(self):
        self.now = datetime(2026, 1, 5, 9, 30)
        self.agents = object()
        self.logs = []

    def get_datetime(self):
        return self.now

    def log_message(self, message):
        self.logs.append(message)


def test_multileg_proof_holds_five_days_before_close():
    tool = _MultilegTool()
    strategy = _MultilegClock()
    open_legs = [{"symbol": "SPY", "side": "sell_to_open"}]
    close_legs = [{"symbol": "SPY", "side": "buy_to_close"}]
    with patch(
        "lumibot.example_strategies.proof_modes._bind_submit_multileg_order",
        return_value=tool,
    ):
        submit_multileg_proof(strategy, open_legs, close_legs)
        assert strategy._multileg_proof_state == "hold"
        assert len(tool.calls) == 1
        strategy.now = datetime(2026, 1, 9, 9, 30)
        submit_multileg_proof(strategy, open_legs, close_legs)
        assert len(tool.calls) == 1
        strategy.now = datetime(2026, 1, 10, 9, 30)
        submit_multileg_proof(strategy, open_legs, close_legs)
    assert len(tool.calls) == 2
    assert "buy_to_close" in tool.calls[1][0]
    assert strategy._multileg_proof_state == "done"


def test_vwap_entry_requires_reclaim_confirmation():
    prompt = build_vwap_system_prompt({})

    assert "require reclaim evidence" in prompt.lower()
    assert "do not skip a clear dip" not in prompt.lower()
