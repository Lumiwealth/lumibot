import json

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas


def test_normalize_routing_sets_cont_future_from_futures():
    routing = {"default": "thetadata", "futures": "ibkr"}
    normalized = RoutedBacktestingPandas._normalize_routing(routing)
    assert normalized["future"] == "ibkr"
    assert normalized["cont_future"] == "ibkr"


def test_normalize_routing_does_not_override_explicit_cont_future():
    routing = {"default": "thetadata", "futures": "ibkr", "cont_future": "polygon"}
    normalized = RoutedBacktestingPandas._normalize_routing(routing)
    assert normalized["future"] == "ibkr"
    assert normalized["cont_future"] == "polygon"


def test_backtesting_data_source_env_common_prod_json():
    # Production often uses this JSON; it should route CONT_FUTURE to the futures provider.
    raw = '{"default":"thetadata","crypto":"ibkr","futures":"ibkr"}'
    routing = json.loads(raw)
    normalized = RoutedBacktestingPandas._normalize_routing(routing)
    assert normalized["future"] == "ibkr"
    assert normalized["cont_future"] == "ibkr"
