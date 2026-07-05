from types import SimpleNamespace

from lumibot.strategies.strategy_executor import StrategyExecutor


def _legacy_lookup(orders, identifier):
    matches = [order for order in orders if order.identifier == identifier]
    if len(matches) > 1:
        return matches
    return matches[0] if matches else None


def test_index_orders_by_identifier_matches_legacy_lookup_shape():
    first = SimpleNamespace(identifier="same")
    second = SimpleNamespace(identifier="same")
    unique = SimpleNamespace(identifier="unique")
    missing_identifier = SimpleNamespace(identifier=None)
    orders = [first, second, unique, missing_identifier]

    indexed = StrategyExecutor._index_orders_by_identifier(orders)

    assert indexed["same"] == _legacy_lookup(orders, "same")
    assert indexed["unique"] is _legacy_lookup(orders, "unique")
    assert indexed[None] is _legacy_lookup(orders, None)
    assert indexed.get("missing") is _legacy_lookup(orders, "missing")
