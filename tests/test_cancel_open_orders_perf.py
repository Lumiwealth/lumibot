import datetime
from unittest.mock import MagicMock

from lumibot.strategies import strategy as strategy_module
from lumibot.strategies import _strategy as base_strategy_module


class _DummyOrder:
    """Lightweight order used to exercise cancellation guards."""

    def __init__(self, identifier: str = "order-1", active: bool = True):
        self.identifier = identifier
        self._active = active

    def is_active(self):
        return self._active


def _build_strategy(mock_broker):
    """Create a minimally configured Strategy instance without running the heavy base __init__."""
    strategy = strategy_module.Strategy.__new__(strategy_module.Strategy)
    strategy.broker = mock_broker
    strategy._name = "TestStrategy"
    strategy.logger = MagicMock()
    strategy.log_message = lambda *args, **kwargs: None
    return strategy


def _build_stats_harness():
    """Instantiate a barebones _Strategy to exercise stats formatting."""
    harness = base_strategy_module._Strategy.__new__(base_strategy_module._Strategy)
    harness._stats_list = []
    harness._stats = None
    harness._stats_dirty = False
    harness._stats_file = None
    harness.logger = MagicMock()
    return harness


def test_cancel_open_orders_skips_broker_when_no_active_orders():
    mock_broker = MagicMock()
    mock_broker.get_tracked_orders.return_value = []
    strategy = _build_strategy(mock_broker)

    strategy.cancel_open_orders()

    mock_broker.cancel_open_orders.assert_not_called()


def test_cancel_open_orders_reuses_prefetched_orders():
    active_order = _DummyOrder("order-42", active=True)
    mock_broker = MagicMock()
    mock_broker.get_tracked_orders.side_effect = AssertionError("should not be called")
    strategy = _build_strategy(mock_broker)

    strategy.cancel_open_orders([active_order])

    mock_broker.cancel_open_orders.assert_called_once()
    passed_orders = mock_broker.cancel_open_orders.call_args[0][1]
    assert passed_orders == [active_order]


def test_format_stats_reuses_dataframe(monkeypatch):
    harness = _build_stats_harness()
    row = {
        "datetime": datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
        "portfolio_value": 100_000.0,
        "cash": 100_000.0,
        "positions": [],
    }
    harness._append_row(row)

    real_dataframe = base_strategy_module.pd.DataFrame
    call_counter = {"count": 0}

    def counting_df(data):
        call_counter["count"] += 1
        return real_dataframe(data)

    monkeypatch.setattr(base_strategy_module.pd, "DataFrame", counting_df)

    harness._format_stats()
    assert call_counter["count"] == 1

    # Subsequent call should reuse cached DataFrame because stats are not dirty.
    harness._format_stats()
    assert call_counter["count"] == 1

    # Mutate stats and ensure DataFrame is rebuilt exactly once more.
    harness._append_row(
        {
            "datetime": datetime.datetime(2024, 1, 1, 0, 1, tzinfo=datetime.timezone.utc),
            "portfolio_value": 100_010.0,
            "cash": 100_010.0,
            "positions": [],
        }
    )
    harness._format_stats()
    assert call_counter["count"] == 2
