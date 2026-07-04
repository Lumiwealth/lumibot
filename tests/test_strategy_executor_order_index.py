from __future__ import annotations

from types import SimpleNamespace

from lumibot.strategies.strategy_executor import StrategyExecutor


def test_index_orders_by_identifier_returns_direct_order_for_unique_ids():
    first = SimpleNamespace(identifier="1")
    second = SimpleNamespace(identifier="2")

    indexed = StrategyExecutor._index_orders_by_identifier([first, second])

    assert indexed == {"1": first, "2": second}


def test_index_orders_by_identifier_preserves_duplicates_for_cleanup_path():
    first = SimpleNamespace(identifier="1")
    duplicate = SimpleNamespace(identifier="1")
    other = SimpleNamespace(identifier="2")

    indexed = StrategyExecutor._index_orders_by_identifier([first, duplicate, other])

    assert indexed["1"] == [first, duplicate]
    assert indexed["2"] is other
