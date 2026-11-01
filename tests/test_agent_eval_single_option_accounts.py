"""Real isolated broker and SQLite fixture state, without any model inference."""
import pytest

from scripts.run_agent_evals import build_fixture
from scripts.agent_eval_production_fixture import ProductionFixture


@pytest.mark.parametrize("name,quantity", [
    ("single_long_option_account", 2), ("single_short_option_account", -2),
])
def test_single_option_fixture_preserves_signed_position_and_exact_contract(name, quantity):
    fixture = ProductionFixture(build_fixture(name))
    try:
        held = [p for p in fixture.strategy.get_positions() if p.asset.symbol != "USD"]
        assert len(held) == 1
        assert held[0].quantity == quantity
        assert held[0].asset.symbol == "SPY"
        assert held[0].asset.strike == 594
        assert str(held[0].asset.right).lower() == "put"
        assert fixture.strategy.get_cash() == 100_000
        assert fixture.strategy.get_orders() == []
    finally:
        fixture.close()
