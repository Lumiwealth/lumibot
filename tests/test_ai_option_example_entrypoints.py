"""Execute example startup configuration without models, data requests or orders."""
import runpy
from pathlib import Path

import pytest

from lumibot import credentials
from lumibot.strategies import Strategy


@pytest.mark.parametrize("filename", [
    "ai_iron_condor.py", "ai_bull_call_spread.py", "ai_call_stock_replacement.py",
])
def test_option_example_backtest_charges_its_declared_contract_fee(monkeypatch, filename):
    calls = []

    def capture(cls, *args, **kwargs):
        calls.append((cls.parameters["estimated_contract_fee"], kwargs))

    monkeypatch.setattr(credentials, "IS_BACKTESTING", True)
    monkeypatch.setattr(Strategy, "backtest", classmethod(capture))
    path = Path(__file__).parents[1] / "lumibot" / "example_strategies" / filename
    runpy.run_path(str(path), run_name="__main__")
    assert len(calls) == 1
    declared_fee, config = calls[0]
    for side in ("buy_trading_fees", "sell_trading_fees"):
        fees = config[side]
        assert len(fees) == 1
        assert float(fees[0].per_contract_fee) == declared_fee
        assert fees[0].flat_fee == fees[0].percent_fee == 0
        assert fees[0].maker and fees[0].taker
