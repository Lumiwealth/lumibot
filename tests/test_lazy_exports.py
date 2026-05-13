import importlib
import inspect
import sys
from unittest.mock import patch


def test_lazy_package_all_exports_resolve():
    modules = [
        "lumibot",
        "lumibot.backtesting",
        "lumibot.brokers",
        "lumibot.components",
        "lumibot.components.agents",
        "lumibot.data_sources",
        "lumibot.entities",
        "lumibot.strategies",
        "lumibot.tools",
        "lumibot.traders",
    ]

    for module_name in modules:
        module = importlib.import_module(module_name)
        for export_name in getattr(module, "__all__", ()):
            getattr(module, export_name)


def test_lazy_package_exports_defer_heavy_submodule_imports():
    # Invariant: importing package namespaces must not import heavy optional
    # backends until their public export is first accessed.
    cases = [
        ("lumibot.backtesting", "BacktestingBroker", "lumibot.backtesting.backtesting_broker"),
        ("lumibot.brokers", "Alpaca", "lumibot.brokers.alpaca"),
        ("lumibot.components", "BuiltinTools", "lumibot.components.agents"),
        ("lumibot.data_sources", "YahooData", "lumibot.data_sources.yahoo_data"),
        ("lumibot.tools", "YahooHelper", "lumibot.tools.yahoo_helper"),
    ]

    for module_name, export_name, target_module in cases:
        sys.modules.pop(target_module, None)
        module = importlib.import_module(module_name)
        module.__dict__.pop(export_name, None)

        assert target_module not in sys.modules
        getattr(module, export_name)
        assert target_module in sys.modules


def test_legacy_star_import_exports_resolve():
    namespace = {}

    exec("from lumibot.tools import *", namespace)
    for name in ("np", "pd", "parse_symbol", "get_logger", "YahooHelper"):
        assert name in namespace
    assert namespace["pd"].__name__ == "pandas"
    assert namespace["np"].__name__ == "numpy"

    exec("from lumibot.entities import *", namespace)
    for name in ("Asset", "Order", "Position", "Data"):
        assert name in namespace

    exec("from lumibot.backtesting import *", namespace)
    for name in ("BacktestingBroker", "PandasDataBacktesting", "YahooDataBacktesting"):
        assert name in namespace


def test_lazy_module_proxies_preserve_module_identity():
    from lumibot.backtesting import backtesting_broker
    from lumibot.entities import bars, data
    from lumibot.strategies import strategy, strategy_executor
    from lumibot.tools import helpers
    from lumibot.tools import ibkr_helper, thetadata_helper

    for value in (
        helpers.pd,
        ibkr_helper.pd,
        thetadata_helper.pd,
        thetadata_helper.mcal,
        thetadata_helper.requests,
        data.pd,
        data.np,
        bars.pd,
        bars.np,
        backtesting_broker.pd,
        backtesting_broker.np,
        strategy.pd,
        strategy.np,
        strategy_executor.pd,
    ):
        assert inspect.ismodule(value)


def test_lazy_module_patch_teardown_forwards_deletion():
    targets = [
        "lumibot.tools.thetadata_helper.requests._lumibot_patch_probe",
        "lumibot.brokers.tradier.requests._lumibot_patch_probe",
        "lumibot.tools.projectx_helpers.requests._lumibot_patch_probe",
        "lumibot.backtesting.routed_backtesting.pd._lumibot_patch_probe",
        "lumibot.backtesting.databento_backtesting_pandas.pd._lumibot_patch_probe",
        "lumibot.data_sources.databento_data_pandas.pd._lumibot_patch_probe",
        "lumibot.indicators.indicators.pd._lumibot_patch_probe",
    ]

    for target in targets:
        with patch(target, object(), create=True):
            pass


def test_lazy_module_patch_teardown_restores_existing_attributes():
    from lumibot.brokers import tradier
    from lumibot.tools import projectx_helpers, thetadata_helper

    target_pairs = [
        (thetadata_helper.requests, "lumibot.tools.thetadata_helper.requests.get"),
        (tradier.requests, "lumibot.brokers.tradier.requests.get"),
        (projectx_helpers.requests, "lumibot.tools.projectx_helpers.requests.get"),
    ]

    for proxy, target in target_pairs:
        original = proxy.get
        with patch(target) as mocked:
            assert proxy.get is mocked
        assert proxy.get is original


def test_lazy_agent_builtin_module_supports_string_patches():
    import lumibot.components as components
    import lumibot.components.agents as agents

    components.__dict__.pop("agents", None)
    agents.__dict__.pop("builtins", None)
    sys.modules.pop("lumibot.components.agents.builtins", None)

    with patch("lumibot.components.agents.builtins.requests.get") as mocked:
        from lumibot.components.agents import builtins

        assert components.agents is agents
        assert builtins.requests.get is mocked


def test_legacy_entities_package_alias_resolves_submodules():
    import lumibot  # noqa: F401
    import entities
    from entities import asset as asset_module
    from entities.asset import Asset
    from entities.order import Order

    assert importlib.util.find_spec("entities.asset") is not None
    assert importlib.util.find_spec("entities.order") is not None
    assert importlib.reload(entities) is entities
    assert importlib.reload(asset_module).Asset is Asset
    assert entities.Asset is Asset
    assert entities.Order is Order
    assert Asset("SPY").symbol == "SPY"
    assert Order.OrderType.MARKET.value == "market"
