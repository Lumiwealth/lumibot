from unittest.mock import MagicMock, patch

import pytest

from lumibot.strategies import Strategy


class _InitialBudgetStrategy(Strategy):
    def on_trading_iteration(self):
        pass


@pytest.mark.parametrize(
    ("cash", "positions_value", "expected_starting_equity"),
    [
        (10_000.0, 0.0, 10_000.0),
        (4_000.0, 6_500.0, 10_500.0),
    ],
)
def test_live_initial_budget_is_defined_as_verified_starting_account_equity(
    cash,
    positions_value,
    expected_starting_equity,
):
    broker = MagicMock()
    broker.name = "synthetic-live"
    broker.quote_assets = set()
    broker.IS_BACKTESTING_BROKER = False
    broker._set_initial_positions = MagicMock()

    def sync_balances(strategy, force_update=True):
        strategy._cash = cash
        strategy._portfolio_value = cash + positions_value
        return True

    with patch.object(Strategy, "update_broker_balances", autospec=True, side_effect=sync_balances):
        strategy = _InitialBudgetStrategy(
            broker=broker,
            analyze_backtest=False,
            parameters={},
        )

    assert strategy.initial_budget == pytest.approx(expected_starting_equity)
    broker._set_initial_positions.assert_called_once_with(strategy)
